import crypto = require("crypto");
import {
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
} from "../common/constants";
import { FILE_UPLOADER, FileUploader } from "../common/r2_file_uploader";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer, VideoTrack } from "../db/schema";
import {
  deleteR2KeyDeletingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  getVideoContainerWritingToFileTaskMetadata,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  insertVideoContainerSyncingTaskStatement,
  updateR2KeyDeletingTaskMetadataStatement,
  updateVideoContainerStatement,
  updateVideoContainerWritingToFileTaskMetadataStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Database, Transaction } from "@google-cloud/spanner";
import { ProcessVideoContainerWritingToFileTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessVideoContainerWritingToFileTaskRequestBody,
  ProcessVideoContainerWritingToFileTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { Ref } from "@selfage/ref";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessVideoContainerWritingToFileTaskHandler extends ProcessVideoContainerWritingToFileTaskHandlerInterface {
  public static create(): ProcessVideoContainerWritingToFileTaskHandler {
    return new ProcessVideoContainerWritingToFileTaskHandler(
      SPANNER_DATABASE,
      S3_CLIENT,
      FILE_UPLOADER,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 365 * 24 * 60 * 60 * 1000; // 1 year
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  public interfereFn: () => void = () => {};
  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private s3Client: Ref<S3Client>,
    private fileUploader: FileUploader,
    private getNow: () => number,
    private generateUuid: () => string,
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
    body: ProcessVideoContainerWritingToFileTaskRequestBody,
  ): Promise<ProcessVideoContainerWritingToFileTaskResponse> {
    loggingPrefix = `${loggingPrefix} Video container writing-to-file task for video container ${body.containerId} and version ${body.version}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessVideoContainerWritingToFileTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getVideoContainerWritingToFileTaskMetadata(transaction, {
        videoContainerWritingToFileTaskContainerIdEq: body.containerId,
        videoContainerWritingToFileTaskVersionEq: body.version,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateVideoContainerWritingToFileTaskMetadataStatement({
          videoContainerWritingToFileTaskContainerIdEq: body.containerId,
          videoContainerWritingToFileTaskVersionEq: body.version,
          setRetryCount: task.videoContainerWritingToFileTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.videoContainerWritingToFileTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessVideoContainerWritingToFileTaskRequestBody,
  ): Promise<void> {
    let videoContainer = await this.getValidVideoContainer(
      this.database,
      body.containerId,
      body.version,
    );
    let masterPlaylistFilename = `${this.generateUuid()}.m3u8`;
    await this.claimR2KeyAndPrepareCleanup(
      loggingPrefix,
      videoContainer.r2RootDirname,
      masterPlaylistFilename,
    );
    try {
      await this.writeContent(
        loggingPrefix,
        videoContainer,
        masterPlaylistFilename,
      );
      await this.finalize(
        loggingPrefix,
        body.containerId,
        videoContainer.masterPlaylist.writingToFile.version,
        masterPlaylistFilename,
      );
    } catch (e) {
      await this.cleanupR2Key(
        loggingPrefix,
        videoContainer.r2RootDirname,
        masterPlaylistFilename,
      );
      throw e;
    }
  }

  private async claimR2KeyAndPrepareCleanup(
    loggingPrefix: string,
    r2RootDir: string,
    masterPlaylistFilename: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessVideoContainerWritingToFileTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming master playlist ${masterPlaylistFilename} and set to clean up at ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        insertR2KeyStatement({ key: `${r2RootDir}/${masterPlaylistFilename}` }),
        insertR2KeyDeletingTaskStatement({
          key: `${r2RootDir}/${masterPlaylistFilename}`,
          retryCount: 0,
          executionTimeMs: delayedTime,
          createdTimeMs: now,
        }),
      ]);
      await transaction.commit();
    });
  }

  private async writeContent(
    loggingPrefix: string,
    videoContainer: VideoContainer,
    masterPlaylistFilename: string,
  ): Promise<void> {
    this.interfereFn();
    let contentParts: Array<string> = [
      await this.getVideoFileContent(
        videoContainer.r2RootDirname,
        videoContainer.videoTracks,
      ),
    ];
    for (let audioTrack of videoContainer.audioTracks) {
      if (!audioTrack.committed) {
        continue;
      }
      contentParts.push(
        `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${audioTrack.committed.name}",DEFAULT=${audioTrack.committed.isDefault ? "YES" : "NO"},AUTOSELECT=NO,URI="${audioTrack.r2TrackDirname}/${LOCAL_PLAYLIST_NAME}"`,
      );
    }
    for (let subtitleTrack of videoContainer.subtitleTracks) {
      if (!subtitleTrack.committed) {
        continue;
      }
      contentParts.push(
        `#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="${subtitleTrack.committed.name}",DEFAULT=${subtitleTrack.committed.isDefault ? "YES" : "NO"},AUTOSELECT=NO,URI="${subtitleTrack.r2TrackDirname}/${LOCAL_PLAYLIST_NAME}"`,
      );
    }
    await this.fileUploader.upload(
      ENV_VARS.r2VideoBucketName,
      `${videoContainer.r2RootDirname}/${masterPlaylistFilename}`,
      contentParts.join("\n"),
    );
  }

  private async getVideoFileContent(
    r2RootDirname: string,
    videoTracks: Array<VideoTrack>,
  ): Promise<string> {
    let videoTrack = videoTracks.find((videoTrack) => videoTrack.committed); // There should be only one with data.
    let response = await this.s3Client.val.send(
      new GetObjectCommand({
        Bucket: ENV_VARS.r2VideoBucketName,
        Key: `${r2RootDirname}/${videoTrack.r2TrackDirname}/${LOCAL_MASTER_PLAYLIST_NAME}`,
      }),
    );
    let content = await response.Body.transformToString();
    return content.replace(
      `${LOCAL_PLAYLIST_NAME}`,
      `${videoTrack.r2TrackDirname}/${LOCAL_PLAYLIST_NAME}`,
    );
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    version: number,
    masterPlaylistFilename: string,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainer(
        transaction,
        containerId,
        version,
      );
      videoContainer.masterPlaylist = {
        syncing: {
          version: version,
          r2Filename: masterPlaylistFilename,
          r2FilenamesToDelete:
            videoContainer.masterPlaylist.writingToFile.r2FilenamesToDelete,
          r2DirnamesToDelete:
            videoContainer.masterPlaylist.writingToFile.r2DirnamesToDelete,
        },
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: containerId,
          setData: videoContainer,
        }),
        insertVideoContainerSyncingTaskStatement({
          containerId,
          version,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        deleteVideoContainerWritingToFileTaskStatement({
          videoContainerWritingToFileTaskContainerIdEq: containerId,
          videoContainerWritingToFileTaskVersionEq: version,
        }),
        deleteR2KeyDeletingTaskStatement({
          r2KeyDeletingTaskKeyEq: `${videoContainer.r2RootDirname}/${masterPlaylistFilename}`,
        }),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainer(
    transaction: Database | Transaction,
    containerId: string,
    version: number,
  ): Promise<VideoContainer> {
    let videoContainerRows = await getVideoContainer(transaction, {
      videoContainerContainerIdEq: containerId,
    });
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} is not found.`);
    }
    let videoContainer = videoContainerRows[0].videoContainerData;
    if (!videoContainer.masterPlaylist.writingToFile) {
      throw newConflictError(
        `Video container ${containerId} is not in writing to file state.`,
      );
    }
    if (videoContainer.masterPlaylist.writingToFile.version !== version) {
      throw newConflictError(
        `Video container ${containerId} is writing to file with a different version than ${version}.`,
      );
    }
    return videoContainer;
  }

  private async cleanupR2Key(
    loggingPrefix: string,
    r2RootDir: string,
    masterPlaylistFilename: string,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up master playlist ${masterPlaylistFilename}.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        updateR2KeyDeletingTaskMetadataStatement({
          r2KeyDeletingTaskKeyEq: `${r2RootDir}/${masterPlaylistFilename}`,
          setRetryCount: 0,
          setExecutionTimeMs:
            this.getNow() +
            ProcessVideoContainerWritingToFileTaskHandler.DELAY_CLEANUP_ON_ERROR_MS,
        }),
      ]);
      await transaction.commit();
    });
  }
}
