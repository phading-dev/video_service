import crypto = require("crypto");
import {
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
} from "../common/constants";
import { R2_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { FILE_UPLOADER, FileUploader } from "../common/r2_file_uploader";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainerData, VideoTrack } from "../db/schema";
import {
  deleteR2KeyDeleteTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertR2KeyDeleteTaskStatement,
  insertR2KeyStatement,
  insertVideoContainerSyncingTaskStatement,
  updateR2KeyDeleteTaskStatement,
  updateVideoContainerStatement,
  updateVideoContainerWritingToFileTaskStatement,
} from "../db/sql";
import {
  ProcessVideoContainerWritingToFileTaskRequestBody,
  ProcessVideoContainerWritingToFileTaskResponse,
} from "../interface";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Database, Transaction } from "@google-cloud/spanner";
import { newConflictError } from "@selfage/http_error";

export class ProcessVideoContainerWritingToFileTaskHandler {
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
  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public doneCallback: () => void = () => {};
  public interfereFn: () => void = () => {};

  public constructor(
    private database: Database,
    private s3Client: S3Client,
    private fileUploader: FileUploader,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: ProcessVideoContainerWritingToFileTaskRequestBody,
  ): Promise<ProcessVideoContainerWritingToFileTaskResponse> {
    loggingPrefix = `${loggingPrefix} Video container writing-to-file task for video container ${body.containerId} and version ${body.version}:`;
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
        ProcessVideoContainerWritingToFileTaskHandler.RETRY_BACKOFF_MS;
      await transaction.batchUpdate([
        updateVideoContainerWritingToFileTaskStatement(
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
        containerId,
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
        insertR2KeyStatement(`${r2RootDir}/${masterPlaylistFilename}`),
        insertR2KeyDeleteTaskStatement(
          `${r2RootDir}/${masterPlaylistFilename}`,
          delayedTime,
          now,
        ),
      ]);
      await transaction.commit();
    });
  }

  private async writeContent(
    loggingPrefix: string,
    videoContainer: VideoContainerData,
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
      R2_VIDEO_REMOTE_BUCKET,
      `${videoContainer.r2RootDirname}/${masterPlaylistFilename}`,
      contentParts.join("\n"),
    );
  }

  private async getVideoFileContent(
    r2RootDirname: string,
    videoTracks: Array<VideoTrack>,
  ): Promise<string> {
    let videoTrack = videoTracks.find((videoTrack) => videoTrack.committed); // There should be only one with data.
    let response = await this.s3Client.send(
      new GetObjectCommand({
        Bucket: R2_VIDEO_REMOTE_BUCKET,
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
      let videoContainer = await this.getValidVideoContainerData(
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
        updateVideoContainerStatement(containerId, videoContainer),
        insertVideoContainerSyncingTaskStatement(
          containerId,
          version,
          now,
          now,
        ),
        deleteVideoContainerWritingToFileTaskStatement(containerId, version),
        deleteR2KeyDeleteTaskStatement(
          `${videoContainer.r2RootDirname}/${masterPlaylistFilename}`,
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
      let delayedTime =
        this.getNow() +
        ProcessVideoContainerWritingToFileTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate([
        updateR2KeyDeleteTaskStatement(
          `${r2RootDir}/${masterPlaylistFilename}`,
          delayedTime,
        ),
      ]);
      await transaction.commit();
    });
  }
}
