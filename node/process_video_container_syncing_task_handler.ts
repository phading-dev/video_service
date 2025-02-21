import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  deleteVideoContainerSyncingTaskStatement,
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  updateVideoContainerStatement,
  updateVideoContainerSyncingTaskStatement,
} from "../db/sql";
import { Database, Transaction } from "@google-cloud/spanner";
import { cacheVideoContainer } from "@phading/product_service_interface/show/node/client";
import { ProcessVideoContainerSyncingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessVideoContainerSyncingTaskRequestBody,
  ProcessVideoContainerSyncingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newConflictError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessVideoContainerSyncingTaskHandler extends ProcessVideoContainerSyncingTaskHandlerInterface {
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
  ) {
    super();
  }

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
  ): Promise<VideoContainer> {
    let videoContainer: VideoContainer;
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
    videoContainer: VideoContainer,
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
    videoContainer: VideoContainer,
  ) {
    await this.syncVideoContainer(loggingPrefix, videoContainer);
    await this.finalize(
      loggingPrefix,
      containerId,
      videoContainer.masterPlaylist.syncing.version,
    );
  }

  private async syncVideoContainer(
    loggingPrefix: string,
    videoContainer: VideoContainer,
  ): Promise<void> {
    console.log(`${loggingPrefix} Syncing video container to show.`);
    let videoTrack = videoContainer.videoTracks.find(
      (track) => track.committed,
    );
    await cacheVideoContainer(this.serviceClient, {
      seasonId: videoContainer.seasonId,
      episodeId: videoContainer.episodeId,
      videoContainer: {
        version: videoContainer.masterPlaylist.syncing.version,
        r2RootDirname: videoContainer.r2RootDirname,
        r2MasterPlaylistFilename:
          videoContainer.masterPlaylist.syncing.r2Filename,
        durationSec: videoTrack.committed.durationSec,
        resolution: videoTrack.committed.resolution,
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
      let r2FilenamesToDelete = syncing.r2FilenamesToDelete;
      let r2DirnamesToDelete = syncing.r2DirnamesToDelete;
      videoContainer.masterPlaylist = {
        synced: {
          version: syncing.version,
          r2Filename: syncing.r2Filename,
        },
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer),
        deleteVideoContainerSyncingTaskStatement(containerId, syncing.version),
        ...r2FilenamesToDelete.map((r2Filename) =>
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${r2Filename}`,
            now,
            now,
          ),
        ),
        ...r2DirnamesToDelete.map((r2Dirname) =>
          insertStorageEndRecordingTaskStatement(
            `${videoContainer.r2RootDirname}/${r2Dirname}`,
            {
              r2Dirname: `${videoContainer.r2RootDirname}/${r2Dirname}`,
              accountId: videoContainer.accountId,
              endTimeMs: now,
            },
            now,
            now,
          ),
        ),
        ...r2DirnamesToDelete.map((r2Dirname) =>
          insertR2KeyDeletingTaskStatement(
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
  ): Promise<VideoContainer> {
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
