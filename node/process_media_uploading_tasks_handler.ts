import crypto = require("crypto");
import { RCLONE_CONFIGURE_FILE } from "../common/constants";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import {
  GetVideoContainerRow,
  deleteMediaUploadingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  getMediaUploadingTaskMetadata,
  getVideoContainer,
  insertGcsKeyDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  insertStorageStartRecordingTaskStatement,
  insertUploadedRecordingTaskStatement,
  updateMediaUploadingTaskMetadataStatement,
  updateR2KeyDeletingTaskMetadataStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { Database } from "@google-cloud/spanner";
import {
  Statement,
  Transaction,
} from "@google-cloud/spanner/build/src/transaction";
import { ProcessMediaUploadingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessMediaUploadingTaskRequestBody,
  ProcessMediaUploadingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export interface VideoDir {
  gcsDirname?: string;
  r2Dirname?: string;
  durationSec?: number;
  resolution?: string;
  totalBytes?: number;
}

export interface AudioDir {
  gcsDirname?: string;
  r2Dirname?: string;
  totalBytes?: number;
}

export class ProcessMediaUploadingTaskHandler extends ProcessMediaUploadingTaskHandlerInterface {
  public static create(): ProcessMediaUploadingTaskHandler {
    return new ProcessMediaUploadingTaskHandler(
      SPANNER_DATABASE,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  public interfereUpload: () => Promise<void> = async () => {};
  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {
    super();
    this.taskHandler = ProcessTaskHandlerWrapper.create(
      this.descriptor,
      2 * 60 * 60 * 1000,
      48 * 60 * 60 * 1000,
    );
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessMediaUploadingTaskRequestBody,
  ): Promise<ProcessMediaUploadingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Media uploading task for video container ${body.containerId} GCS dirname ${body.gcsDirname}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessMediaUploadingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getMediaUploadingTaskMetadata(transaction, {
        mediaUploadingTaskContainerIdEq: body.containerId,
        mediaUploadingTaskGcsDirnameEq: body.gcsDirname,
      });
      if (rows.length === 0) {
        throw newBadRequestError(`Task is not found.`);
      }
      let task = rows[0];
      let delayedExecutionTime =
        this.getNow() +
        this.taskHandler.getBackoffTime(task.mediaUploadingTaskRetryCount);
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedExecutionTime}.`,
      );
      await transaction.batchUpdate([
        updateMediaUploadingTaskMetadataStatement({
          mediaUploadingTaskContainerIdEq: body.containerId,
          mediaUploadingTaskGcsDirnameEq: body.gcsDirname,
          setRetryCount: task.mediaUploadingTaskRetryCount + 1,
          setExecutionTimeMs: delayedExecutionTime,
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessMediaUploadingTaskRequestBody,
  ): Promise<void> {
    let { videoContainerData } = await this.getValidVideoContainerData(
      this.database,
      body.containerId,
      body.gcsDirname,
    );

    // Create new dirs so that if same task got executed twice, they won't overwrite each other.
    let videoDirOptional: Array<VideoDir> = [];
    if (videoContainerData.processing.mediaUploading.gcsVideoDirname) {
      videoDirOptional.push({
        gcsDirname:
          videoContainerData.processing.mediaUploading.gcsVideoDirname,
        r2Dirname: this.generateUuid(),
        durationSec:
          videoContainerData.processing.mediaUploading.videoInfo.durationSec,
        resolution:
          videoContainerData.processing.mediaUploading.videoInfo.resolution,
      });
    }
    let audioDirs =
      videoContainerData.processing.mediaUploading.gcsAudioDirnames.map(
        (gcsAudioDirname) => ({
          gcsDirname: gcsAudioDirname,
          r2Dirname: this.generateUuid(),
        }),
      );

    await this.claimR2KeysAndPrepareCleanup(
      loggingPrefix,
      videoContainerData.r2RootDirname,
      videoDirOptional,
      audioDirs,
    );
    try {
      await this.uploadDirs(
        loggingPrefix,
        body.gcsDirname,
        videoContainerData.r2RootDirname,
        videoDirOptional,
        audioDirs,
      );
      await this.finalize(
        loggingPrefix,
        body.containerId,
        body.gcsDirname,
        videoDirOptional,
        audioDirs,
      );
    } catch (e) {
      await this.cleanupR2Keys(
        loggingPrefix,
        videoContainerData.r2RootDirname,
        videoDirOptional,
        audioDirs,
      );
      throw e;
    }
  }

  private async claimR2KeysAndPrepareCleanup(
    loggingPrefix: string,
    r2RootDir: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime = now + ProcessMediaUploadingTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming video dir [${videoDirOptional.map((dir) => r2RootDir + "/" + dir.r2Dirname).join()}] and audio dirs [${audioDirs.map((dir) => r2RootDir + "/" + dir.r2Dirname).join()}] and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      for (let videoDir of videoDirOptional) {
        statements.push(
          insertR2KeyStatement({ key: `${r2RootDir}/${videoDir.r2Dirname}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${videoDir.r2Dirname}`,
            retryCount: 0,
            executionTimeMs: delayedTime,
            createdTimeMs: now,
          }),
        );
      }
      for (let audioDir of audioDirs) {
        statements.push(
          insertR2KeyStatement({ key: `${r2RootDir}/${audioDir.r2Dirname}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${audioDir.r2Dirname}`,
            retryCount: 0,
            executionTimeMs: delayedTime,
            createdTimeMs: now,
          }),
        );
      }
      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
  }

  private async uploadDirs(
    loggingPrefix: string,
    gcsDirname: string,
    r2RootDir: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Start uploading.`);
    await this.interfereUpload();
    for (let videoDir of videoDirOptional) {
      await spawnAsync(`${loggingPrefix} Uploading gcs video dir`, "rclone", [
        "copy",
        `gcs_remote:${ENV_VARS.gcsVideoBucketName}/${gcsDirname}/${videoDir.gcsDirname}`,
        `r2_remote:${ENV_VARS.r2VideoBucketName}/${r2RootDir}/${videoDir.r2Dirname}`,
        "--transfers",
        "16",
        "--checkers",
        "8",
        "--log-level",
        "ERROR",
        `--config`,
        `${RCLONE_CONFIGURE_FILE}`,
      ]);
    }
    for (let audioDir of audioDirs) {
      await spawnAsync(`${loggingPrefix} Uploading gcs audio dir`, "rclone", [
        "copy",
        `gcs_remote:${ENV_VARS.gcsVideoBucketName}/${gcsDirname}/${audioDir.gcsDirname}`,
        `r2_remote:${ENV_VARS.r2VideoBucketName}/${r2RootDir}/${audioDir.r2Dirname}`,
        "--transfers",
        "16",
        "--checkers",
        "8",
        "--log-level",
        "ERROR",
        `--config`,
        `${RCLONE_CONFIGURE_FILE}`,
      ]);
    }
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsDirname: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let { videoContainerAccountId, videoContainerData } =
        await this.getValidVideoContainerData(
          transaction,
          containerId,
          gcsDirname,
        );
      videoContainerData.processing = undefined; // Processing completed.
      videoDirOptional.forEach((videoDir) => {
        videoContainerData.videoTracks.push({
          r2TrackDirname: videoDir.r2Dirname,
          totalBytes: videoDir.totalBytes,
          durationSec: videoDir.durationSec,
          resolution: videoDir.resolution,
          staging: {
            toAdd: true,
          },
        });
      });
      let existingAudios = videoContainerData.audioTracks.length;
      audioDirs.forEach((audioDir, index) => {
        videoContainerData.audioTracks.push({
          r2TrackDirname: audioDir.r2Dirname,
          totalBytes: audioDir.totalBytes,
          staging: {
            toAdd: {
              name: `${existingAudios + index + 1}`,
              isDefault: existingAudios + index === 0, // If this is the first audio track
            },
          },
        });
      });
      let totalProcessedBytes =
        videoDirOptional.reduce(
          (sum, videoDir) => sum + videoDir.totalBytes,
          0,
        ) + audioDirs.reduce((sum, audioDir) => sum + audioDir.totalBytes, 0);

      let now = this.getNow();
      // TODO: Add a task to send notification to users when completed.
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: containerId,
          setData: videoContainerData,
        }),
        deleteMediaUploadingTaskStatement({
          mediaUploadingTaskContainerIdEq: containerId,
          mediaUploadingTaskGcsDirnameEq: gcsDirname,
        }),
        insertGcsKeyDeletingTaskStatement({
          key: gcsDirname,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        insertUploadedRecordingTaskStatement({
          gcsKey: gcsDirname,
          payload: {
            accountId: videoContainerAccountId,
            totalBytes: totalProcessedBytes,
          },
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        ...videoDirOptional.map((videoDir) =>
          insertStorageStartRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${videoDir.r2Dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              totalBytes: videoDir.totalBytes,
              startTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          insertStorageStartRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${audioDir.r2Dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              totalBytes: audioDir.totalBytes,
              startTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...videoDirOptional.map((videoDir) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${videoContainerData.r2RootDirname}/${videoDir.r2Dirname}`,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${videoContainerData.r2RootDirname}/${audioDir.r2Dirname}`,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainerData(
    transaction: Database | Transaction,
    containerId: string,
    gcsDirname: string,
  ): Promise<GetVideoContainerRow> {
    let videoContainerRows = await getVideoContainer(transaction, {
      videoContainerContainerIdEq: containerId,
    });
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} is not found.`);
    }
    let videoContainer = videoContainerRows[0];
    if (!videoContainer.videoContainerData.processing?.mediaUploading) {
      throw newConflictError(
        `Video container ${containerId} is not in media uploading state.`,
      );
    }
    if (
      videoContainer.videoContainerData.processing.mediaUploading.gcsDirname !==
      gcsDirname
    ) {
      throw newConflictError(
        `Video container ${containerId} is uploading a different directory than ${gcsDirname}.`,
      );
    }
    return videoContainer;
  }

  private async cleanupR2Keys(
    loggingPrefix: string,
    r2RootDirname: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up video dir [${videoDirOptional.map((dir) => r2RootDirname + "/" + dir.r2Dirname).join()}] and audio dirs [${audioDirs.map((dir) => r2RootDirname + "/" + dir.r2Dirname).join()}] in ${ProcessMediaUploadingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessMediaUploadingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate([
        ...videoDirOptional.map((videoDir) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${videoDir.r2Dirname}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${audioDir.r2Dirname}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }
}
