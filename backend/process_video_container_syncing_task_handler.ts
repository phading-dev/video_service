import { syncVideoContainer } from "../client";
import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainerData } from "../db/schema";
import {
  deleteVideoContainerSyncingTaskStatement,
  getVideoContainer,
  insertR2KeyDeleteTaskStatement,
  updateVideoContainerStatement,
  updateVideoContainerSyncingTaskStatement,
} from "../db/sql";
import {
  ProcessVideoContainerSyncingTaskRequestBody,
  ProcessVideoContainerSyncingTaskResponse,
} from "../interface";
import { Database, Transaction } from "@google-cloud/spanner";
import { newConflictError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessVideoContainerSyncingTaskHandler {
  public static create(): ProcessVideoContainerSyncingTaskHandler {
    return new ProcessVideoContainerSyncingTaskHandler(
      SPANNER_DATABASE,
      SERVICE_CLIENT,
      () => Date.now(),
    );
  }

  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public doneCallback: () => void = () => {};

  public constructor(
    private database: Database,
    private serviceClient: NodeServiceClient,
    private getNow: () => number,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: ProcessVideoContainerSyncingTaskRequestBody,
  ): Promise<ProcessVideoContainerSyncingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Video container syncing task for video container ${body.containerId} and version ${body.version}:`;
    let videoContainer = await this.getPayloadAndDelayExecutionTime(
      loggingPrefix,
      body.containerId,
      body.version,
    );
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.containerId,
      videoContainer,
    );
    return {};
  }

  private async getPayloadAndDelayExecutionTime(
    loggingPrefix: string,
    containerId: string,
    version: number,
  ): Promise<VideoContainerData> {
    let videoContainer: VideoContainerData;
    await this.database.runTransactionAsync(async (transaction) => {
      videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        version,
      );
      let delayedTime =
        this.getNow() +
        ProcessVideoContainerSyncingTaskHandler.RETRY_BACKOFF_MS;
      await transaction.batchUpdate([
        updateVideoContainerSyncingTaskStatement(
          containerId,
          version,
          delayedTime,
        ),
      ]);
      await transaction.commit();
    });
    return videoContainer;
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    containerId: string,
    videoContainer: VideoContainerData,
  ) {
    console.log(`${loggingPrefix} Task starting.`);
    try {
      await this.startProcessing(loggingPrefix, containerId, videoContainer);
      console.log(`${loggingPrefix} Task completed!`);
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }

  private async startProcessing(
    loggingPrefix: string,
    containerId: string,
    videoContainer: VideoContainerData,
  ) {
    await this.syncVideoContainer(loggingPrefix, containerId, videoContainer);
    await this.finalize(
      loggingPrefix,
      containerId,
      videoContainer.masterPlaylist.syncing.version,
    );
  }

  private async syncVideoContainer(
    loggingPrefix: string,
    containerId: string,
    videoContainer: VideoContainerData,
  ): Promise<void> {
    console.log(`${loggingPrefix} Syncing video container to show.`);
    let videoTrack = videoContainer.videoTracks.find(
      (track) => track.committed,
    );
    await syncVideoContainer(this.serviceClient, {
      showId: videoContainer.showId,
      containerId,
      container: {
        version: videoContainer.masterPlaylist.syncing.version,
        r2RootDirname: videoContainer.r2RootDirname,
        r2MasterPlaylistFilename:
          videoContainer.masterPlaylist.syncing.r2Filename,
        durationSec: videoTrack.committed.durationSec,
        resolution: videoTrack.committed.resolution,
        audioTracks: videoContainer.audioTracks
          .filter((track) => track.committed)
          .map((track) => {
            return {
              name: track.committed.name,
              isDefault: track.committed.isDefault,
            };
          }),
        subtitleTracks: videoContainer.subtitleTracks
          .filter((track) => track.committed)
          .map((track) => {
            return {
              name: track.committed.name,
              isDefault: track.committed.isDefault,
            };
          }),
      },
    });
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    version: number,
  ) {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        version,
      );
      let syncing = videoContainer.masterPlaylist.syncing;
      let r2FilenamesToDelete = syncing.r2DirnamesToDelete;
      let r2DirnamesToDelete = syncing.r2FilenamesToDelete;
      videoContainer.masterPlaylist = {
        synced: {
          version: syncing.version,
          r2Filename: syncing.r2Filename,
        },
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(containerId, videoContainer),
        deleteVideoContainerSyncingTaskStatement(containerId, syncing.version),
        ...r2FilenamesToDelete.map((r2Filename) =>
          insertR2KeyDeleteTaskStatement(
            `${videoContainer.r2RootDirname}/${r2Filename}`,
            now,
            now,
          ),
        ),
        ...r2DirnamesToDelete.map((r2Dirname) =>
          insertR2KeyDeleteTaskStatement(
            `${videoContainer.r2RootDirname}/${r2Dirname}`,
            now,
            now,
          ),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainerData(
    transaction: Transaction,
    containerId: string,
    version: number,
  ): Promise<VideoContainerData> {
    let videoContainerRows = await getVideoContainer(transaction, containerId);
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} not found`);
    }
    let videoContainer = videoContainerRows[0].videoContainerData;
    if (!videoContainer.masterPlaylist.syncing) {
      throw newConflictError(
        `Video container ${containerId} is not in syncing state.`,
      );
    }
    if (videoContainer.masterPlaylist.syncing.version !== version) {
      throw newConflictError(
        `Video container ${containerId} is syncing with a different version than ${version}.`,
      );
    }
    return videoContainer;
  }
}
