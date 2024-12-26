import crypto = require("crypto");
import {
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
} from "../common/constants";
import {
  GCS_VIDEO_LOCAL_DIR,
  MEDIA_TEMP_DIR,
  R2_VIDEO_REMOTE_BUCKET,
} from "../common/env_vars";
import { HLS_SEGMENT_TIME } from "../common/params";
import { DirectoryStreamUploader } from "../common/r2_directory_stream_uploader";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import { VideoContainer } from "../db/schema";
import {
  deleteMediaFormattingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  updateMediaFormattingTaskStatement,
  updateR2KeyDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import {
  Statement,
  Transaction,
} from "@google-cloud/spanner/build/src/transaction";
import { ProcessMediaFormattingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessMediaFormattingTaskRequestBody,
  ProcessMediaFormattingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/processing_failure_reason";
import { BlockingLoop } from "@selfage/blocking_loop";
import { newConflictError } from "@selfage/http_error";
import { mkdir, rm } from "fs/promises";

export interface VideoInfo {
  durationSec: number;
  resolution: string;
}

export interface DirAndSize {
  dirname: string;
  totalBytes: number;
}

export class ProcessMediaFormattingTaskHandler extends ProcessMediaFormattingTaskHandlerInterface {
  public static create(): ProcessMediaFormattingTaskHandler {
    return new ProcessMediaFormattingTaskHandler(
      SPANNER_DATABASE,
      BlockingLoop.createWithTimeout(),
      DirectoryStreamUploader.create,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 365 * 24 * 60 * 60 * 1000; // 1 year
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  private static RETRY_BACKOFF_MS = 8 * 60 * 1000;
  private static INTERVAL_TO_BACKOFF_RETRY_MS = 5 * 60 * 1000;
  private static VIDEO_CODEC = "h264";
  private static AUDIO_CODEC = "aac";

  public doneCallback: () => void = () => {};
  public interfereFormat: () => Promise<void> = () => Promise.resolve();

  public constructor(
    private database: Database,
    private blockingLoop: BlockingLoop,
    private createDirectoryStreamUploader: (
      loggingPrefix: string,
      localDir: string,
      remoteBucket: string,
      remoteDir: string,
    ) => DirectoryStreamUploader,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessMediaFormattingTaskRequestBody,
  ): Promise<ProcessMediaFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Media formatting task for video container ${body.containerId} GCS filename ${body.gcsFilename}:`;
    let { r2RootDirname } = await this.getPayloadAndDelayExecutionTime(
      loggingPrefix,
      body.containerId,
      body.gcsFilename,
    );
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.containerId,
      body.gcsFilename,
      r2RootDirname,
    );
    return {};
  }

  private async getPayloadAndDelayExecutionTime(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
  ): Promise<{
    r2RootDirname: string;
  }> {
    let r2RootDirname: string;
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      r2RootDirname = videoContainer.r2RootDirname;
      let delayedTime =
        this.getNow() + ProcessMediaFormattingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateMediaFormattingTaskStatement(
          containerId,
          gcsFilename,
          delayedTime,
        ),
      ]);
      await transaction.commit();
    });
    return {
      r2RootDirname,
    };
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDir: string,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task starting.`);
    try {
      await this.startProcessingAndCleanupTempDir(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDir,
      );
      console.log(`${loggingPrefix} Task completed!`);
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }

  private async startProcessingAndCleanupTempDir(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDirname: string,
  ): Promise<void> {
    let tempDir = `${MEDIA_TEMP_DIR}/${gcsFilename}/${this.generateUuid()}`;
    try {
      await this.startProcessing(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDirname,
        tempDir,
      );
    } finally {
      await rm(tempDir, {
        recursive: true,
        force: true,
      });
    }
  }

  private async startProcessing(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDir: string,
    tempDir: string,
  ): Promise<void> {
    let { failures, videoInfo, audios } = await this.validateAndExtractInfo(
      loggingPrefix,
      gcsFilename,
    );
    if (failures.length > 0) {
      await this.reportFailures(
        loggingPrefix,
        containerId,
        gcsFilename,
        failures,
      );
      return;
    }

    // Create new R2 dirs so that if same task got executed twice, they won't overwrite each other.
    let videoDirOptional: Array<string> = videoInfo
      ? [this.generateUuid()]
      : [];
    let audioDirs = new Array<string>();
    for (let i = 0; i < audios; i++) {
      audioDirs.push(this.generateUuid());
    }
    await this.claimR2KeyAndPrepareCleanup(
      loggingPrefix,
      r2RootDir,
      videoDirOptional,
      audioDirs,
    );
    try {
      this.blockingLoop
        .setInterval(
          ProcessMediaFormattingTaskHandler.INTERVAL_TO_BACKOFF_RETRY_MS,
        )
        .setAction(() =>
          this.delayExecutionTime(loggingPrefix, containerId, gcsFilename),
        )
        .start();
      let { videoDirAndSizeOptional, audioDirsAndSizes } =
        await this.toHlsFormat(
          loggingPrefix,
          gcsFilename,
          r2RootDir,
          tempDir,
          videoDirOptional,
          audioDirs,
        );
      await this.blockingLoop.stop();
      await this.finalize(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDir,
        videoDirAndSizeOptional,
        videoInfo,
        audioDirsAndSizes,
      );
    } catch (e) {
      await this.cleanupR2Keys(
        loggingPrefix,
        r2RootDir,
        videoDirOptional,
        audioDirs,
      );
      throw e;
    }
  }

  private async validateAndExtractInfo(
    loggingPrefix: string,
    gcsFilename: string,
  ): Promise<{
    failures: Array<ProcessingFailureReason>;
    videoInfo: VideoInfo | undefined;
    audios: number;
  }> {
    console.log(`${loggingPrefix} Validating codecs and extracting metadata.`);
    let stdout = await spawnAsync(
      `${loggingPrefix} When getting codec of the video container file:`,
      "ffprobe",
      [
        "-v",
        "error",
        "-show_entries",
        "stream=codec_name,codec_type,width,height,duration",
        "-of",
        "json",
        `${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`,
      ],
    );
    console.log(
      `${loggingPrefix} Extracted metadata of the video located at GCS path ${gcsFilename} with output:`,
      stdout,
    );
    /* output should be like 
      {
        "streams": [{
          "codec_name": "h264",
          "codec_type": "video",
          "width": 640,
          "height": 360,
          "duration": "240.72"
        }, {
          "codec_name": "aac",
          "codec_type": "audio"
        }]
      }
    */
    let videos = 0;
    let audios = 0;
    let videoDurationSec: number;
    let videoResolution: string;
    let failures = new Array<ProcessingFailureReason>();
    JSON.parse(stdout)["streams"].forEach((stream: any, index: number) => {
      switch (stream["codec_type"]) {
        case "video": {
          if (videos < 1) {
            let codec = stream["codec_name"];
            if (codec !== ProcessMediaFormattingTaskHandler.VIDEO_CODEC) {
              console.log(
                `${loggingPrefix} ${index}th stream is a video with codec ${codec} but requires ${ProcessMediaFormattingTaskHandler.VIDEO_CODEC}.`,
              );
              failures.push(ProcessingFailureReason.VIDEO_CODEC_REQUIRES_H264);
            }
            videoDurationSec = parseInt(stream["duration"]);
            videoResolution = `${stream["width"]}x${stream["height"]}`;
          } // Ignores other video streams.
          videos++;
          break;
        }
        case "audio": {
          let codec = stream["codec_name"];
          if (codec !== ProcessMediaFormattingTaskHandler.AUDIO_CODEC) {
            console.log(
              `${loggingPrefix} ${index}th stream is a audio with codec ${codec} but requires ${ProcessMediaFormattingTaskHandler.AUDIO_CODEC}.`,
            );
            failures.push(ProcessingFailureReason.AUDIO_CODEC_REQUIRES_AAC);
          }
          audios++;
          break;
        }
        // Ignores other types of stream.
      }
    });
    return {
      failures,
      videoInfo:
        videos > 0
          ? { durationSec: videoDurationSec, resolution: videoResolution }
          : undefined,
      audios,
    };
  }

  private async reportFailures(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    failures: Array<ProcessingFailureReason>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Reporting failures: ${failures.map((failure) => ProcessingFailureReason[failure]).join()}`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      videoContainer.processing = undefined;
      videoContainer.lastProcessingFailures = failures;
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer),
        deleteMediaFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileDeletingTaskStatement(gcsFilename, "", now, now),
      ]);
      await transaction.commit();
    });
  }

  private async claimR2KeyAndPrepareCleanup(
    loggingPrefix: string,
    r2RootDir: string,
    videoDirOptional: Array<string>,
    audioDirs: Array<string>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming video dir [${videoDirOptional.join()}] and audio dirs [${audioDirs.join()}] and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      for (let videoDir of videoDirOptional) {
        statements.push(
          insertR2KeyStatement(`${r2RootDir}/${videoDir}`),
          insertR2KeyDeletingTaskStatement(
            `${r2RootDir}/${videoDir}`,
            delayedTime,
            now,
          ),
        );
      }
      for (let audioDir of audioDirs) {
        statements.push(
          insertR2KeyStatement(`${r2RootDir}/${audioDir}`),
          insertR2KeyDeletingTaskStatement(
            `${r2RootDir}/${audioDir}`,
            delayedTime,
            now,
          ),
        );
      }
      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
  }

  private async delayExecutionTime(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessMediaFormattingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Task not finished yet. Delaying all tasks execution time to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateMediaFormattingTaskStatement(
          containerId,
          gcsFilename,
          delayedTime,
        ),
      ]);
      await transaction.commit();
    });
  }

  private async toHlsFormat(
    loggingPrefix: string,
    gcsFilename: string,
    r2RootDirname: string,
    tempDir: string,
    videoDirOptional: Array<string>,
    audioDirs: Array<string>,
  ): Promise<{
    videoDirAndSizeOptional: Array<DirAndSize>;
    audioDirsAndSizes: Array<DirAndSize>;
  }> {
    console.log(`${loggingPrefix} Start HLS formatting.`);
    await this.interfereFormat();
    let formattingArgs: Array<string> = [
      "-i",
      `${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`,
      "-loglevel",
      "error",
    ];
    videoDirOptional.forEach((videoDir) => {
      formattingArgs.push(
        "-map",
        "0:v:0",
        "-c:v",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        `${HLS_SEGMENT_TIME}`,
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        `${tempDir}/${videoDir}/%d.ts`,
        "-master_pl_name",
        `${LOCAL_MASTER_PLAYLIST_NAME}`,
        `${tempDir}/${videoDir}/${LOCAL_PLAYLIST_NAME}`,
      );
    });
    audioDirs.forEach((audioDir, i) => {
      formattingArgs.push(
        "-map",
        `0:a:${i}`,
        "-c:a",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        `${HLS_SEGMENT_TIME}`,
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        `${tempDir}/${audioDir}/%d.ts`,
        `${tempDir}/${audioDir}/${LOCAL_PLAYLIST_NAME}`,
      );
    });
    let videoDirUploaderOptional = new Array<DirectoryStreamUploader>();
    for (let videoDir of videoDirOptional) {
      await mkdir(`${tempDir}/${videoDir}`, {
        recursive: true,
      });
      videoDirUploaderOptional.push(
        this.createDirectoryStreamUploader(
          loggingPrefix,
          `${tempDir}/${videoDir}`,
          R2_VIDEO_REMOTE_BUCKET,
          `${r2RootDirname}/${videoDir}`,
        ).start(),
      );
    }
    let audioDirUploaders = new Array<DirectoryStreamUploader>();
    for (let audioDir of audioDirs) {
      await mkdir(`${tempDir}/${audioDir}`, {
        recursive: true,
      });
      audioDirUploaders.push(
        this.createDirectoryStreamUploader(
          loggingPrefix,
          `${tempDir}/${audioDir}`,
          R2_VIDEO_REMOTE_BUCKET,
          `${r2RootDirname}/${audioDir}`,
        ).start(),
      );
    }
    await spawnAsync(
      `${loggingPrefix} When formatting video to HLS:`,
      "ffmpeg",
      formattingArgs,
    );

    let videoDirAndSizeOptional = new Array<DirAndSize>();
    for (let i = 0; i < videoDirOptional.length; i++) {
      let totalBytes = await videoDirUploaderOptional[i].flush();
      videoDirAndSizeOptional.push({
        dirname: videoDirOptional[i],
        totalBytes,
      });
    }
    let audioDirsAndSizes = new Array<DirAndSize>();
    for (let i = 0; i < audioDirs.length; i++) {
      let totalBytes = await audioDirUploaders[i].flush();
      audioDirsAndSizes.push({
        dirname: audioDirs[i],
        totalBytes,
      });
    }
    return { videoDirAndSizeOptional, audioDirsAndSizes };
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDirname: string,
    videoDirAndSizeOptional: Array<DirAndSize>,
    videoInfo: VideoInfo | undefined,
    audioDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      videoContainer.processing = undefined; // Processing completed.
      videoDirAndSizeOptional.forEach((videoDirAndSize) => {
        videoContainer.videoTracks.push({
          r2TrackDirname: videoDirAndSize.dirname,
          staging: {
            toAdd: {
              totalBytes: videoDirAndSize.totalBytes,
              durationSec: videoInfo.durationSec,
              resolution: videoInfo.resolution,
            },
          },
        });
      });
      let existingAudios = videoContainer.audioTracks.length;
      audioDirsAndSizes.forEach((audioDirAndSize, index) => {
        videoContainer.audioTracks.push({
          r2TrackDirname: audioDirAndSize.dirname,
          staging: {
            toAdd: {
              name: `${existingAudios + index + 1}`,
              isDefault: existingAudios + index === 0, // If this is the first audio track
              totalBytes: audioDirAndSize.totalBytes,
            },
          },
        });
      });

      let now = this.getNow();
      // TODO: Add a task to send notification to users when completed.
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer),
        deleteMediaFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileDeletingTaskStatement(gcsFilename, "", now, now),
        ...videoDirAndSizeOptional.map((videoDirAndSize) =>
          deleteR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${videoDirAndSize.dirname}`,
          ),
        ),
        ...audioDirsAndSizes.map((audioDirAndSize) =>
          deleteR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${audioDirAndSize.dirname}`,
          ),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainerData(
    transaction: Transaction,
    containerId: string,
    gcsFilename: string,
  ): Promise<VideoContainer> {
    let videoContainerRows = await getVideoContainer(transaction, containerId);
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} is not found.`);
    }
    let videoContainer = videoContainerRows[0].videoContainerData;
    if (!videoContainer.processing?.media?.formatting) {
      throw newConflictError(
        `Video container ${containerId} is not in media formatting state.`,
      );
    }
    if (
      videoContainer.processing.media.formatting.gcsFilename !== gcsFilename
    ) {
      throw newConflictError(
        `Video container ${containerId} is formatting a different file than ${gcsFilename}.`,
      );
    }
    return videoContainer;
  }

  private async cleanupR2Keys(
    loggingPrefix: string,
    r2RootDirname: string,
    videoDirOptional: Array<string>,
    audioDirs: Array<string>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up video dir [${videoDirOptional.join()}] and audio dirs [${audioDirs.join()}] in ${ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate([
        ...videoDirOptional.map((videoDir) =>
          updateR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${videoDir}`,
            delayedTime,
          ),
        ),
        ...audioDirs.map((audioDir) =>
          updateR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${audioDir}`,
            delayedTime,
          ),
        ),
      ]);
      await transaction.commit();
    });
  }
}
