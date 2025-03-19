import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GetVideoContainerRow,
  deleteVideoContainerSyncingTaskStatement,
  getVideoContainer,
  getVideoContainerSyncingTaskMetadata,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  updateVideoContainerStatement,
  updateVideoContainerSyncingTaskMetadataStatement,
} from "../db/sql";
import { Database, Transaction } from "@google-cloud/spanner";
import { newCacheVideoContainerRequest } from "@phading/product_service_interface/show/node/client";
import { ProcessVideoContainerSyncingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessVideoContainerSyncingTaskRequestBody,
  ProcessVideoContainerSyncingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessVideoContainerSyncingTaskHandler extends ProcessVideoContainerSyncingTaskHandlerInterface {
  public static create(): ProcessVideoContainerSyncingTaskHandler {
    return new ProcessVideoContainerSyncingTaskHandler(
      SPANNER_DATABASE,
      SERVICE_CLIENT,
      () => Date.now(),
    );
  }

  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private serviceClient: NodeServiceClient,
    private getNow: () => number,
  ) {
    super();
    this.taskHandler = ProcessTaskHandlerWrapper.create(
      this.descriptor,
      5 * 60 * 1000,
      24 * 60 * 60 * 1000,
    );
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessVideoContainerSyncingTaskRequestBody,
  ): Promise<ProcessVideoContainerSyncingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Video container syncing task for video container ${body.containerId} and version ${body.version}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessVideoContainerSyncingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getVideoContainerSyncingTaskMetadata(transaction, {
        videoContainerSyncingTaskContainerIdEq: body.containerId,
        videoContainerSyncingTaskVersionEq: body.version,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateVideoContainerSyncingTaskMetadataStatement({
          videoContainerSyncingTaskContainerIdEq: body.containerId,
          videoContainerSyncingTaskVersionEq: body.version,
          setRetryCount: task.videoContainerSyncingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.videoContainerSyncingTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessVideoContainerSyncingTaskRequestBody,
  ): Promise<void> {
    let {
      videoContainerSeasonId,
      videoContainerEpisodeId,
      videoContainerData,
    } = await this.getValidVideoContainerData(
      this.database,
      body.containerId,
      body.version,
    );
    let videoTrack = videoContainerData.videoTracks.find(
      (track) => track.committed,
    );
    await this.serviceClient.send(
      newCacheVideoContainerRequest({
        seasonId: videoContainerSeasonId,
        episodeId: videoContainerEpisodeId,
        videoContainer: {
          version: videoContainerData.masterPlaylist.syncing.version,
          r2RootDirname: videoContainerData.r2RootDirname,
          r2MasterPlaylistFilename:
            videoContainerData.masterPlaylist.syncing.r2Filename,
          durationSec: videoTrack.committed.durationSec,
          resolution: videoTrack.committed.resolution,
        },
      }),
    );

    await this.database.runTransactionAsync(async (transaction) => {
      let { videoContainerAccountId, videoContainerData } =
        await this.getValidVideoContainerData(
          transaction,
          body.containerId,
          body.version,
        );
      let syncing = videoContainerData.masterPlaylist.syncing;
      let r2FilenamesToDelete = syncing.r2FilenamesToDelete;
      let r2DirnamesToDelete = syncing.r2DirnamesToDelete;
      videoContainerData.masterPlaylist = {
        synced: {
          version: syncing.version,
          r2Filename: syncing.r2Filename,
        },
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        deleteVideoContainerSyncingTaskStatement({
          videoContainerSyncingTaskContainerIdEq: body.containerId,
          videoContainerSyncingTaskVersionEq: syncing.version,
        }),
        ...r2FilenamesToDelete.map((r2Filename) =>
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${r2Filename}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...r2DirnamesToDelete.map((r2Dirname) =>
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...r2DirnamesToDelete.map((r2Dirname) =>
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainerData(
    transaction: Database | Transaction,
    containerId: string,
    version: number,
  ): Promise<GetVideoContainerRow> {
    let videoContainerRows = await getVideoContainer(transaction, {
      videoContainerContainerIdEq: containerId,
    });
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} not found`);
    }
    let videoContainer = videoContainerRows[0];
    if (!videoContainer.videoContainerData.masterPlaylist.syncing) {
      throw newConflictError(
        `Video container ${containerId} is not in syncing state.`,
      );
    }
    if (
      videoContainer.videoContainerData.masterPlaylist.syncing.version !==
      version
    ) {
      throw newConflictError(
        `Video container ${containerId} is syncing with a different version than ${version}.`,
      );
    }
    return videoContainer;
  }
}
