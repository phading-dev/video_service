import crypto = require("crypto");
import {
  HLS_SEGMENT_TIME,
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
  MEDIA_TEMP_DIR,
} from "../common/constants";
import { DirectoryStreamUploader } from "../common/r2_directory_stream_uploader";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import {
  GetVideoContainerRow,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  getMediaFormattingTaskMetadata,
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  insertStorageStartRecordingTaskStatement,
  updateMediaFormattingTaskMetadataStatement,
  updateR2KeyDeletingTaskMetadataStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
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
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";
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
  private static INTERVAL_TO_DELAY_RETRY_MS = 5 * 60 * 1000;
  private static VIDEO_CODEC = "h264";
  private static AUDIO_CODEC = "aac";
  public interfereFormat: () => Promise<void> = async () => {};
  private taskHandler: ProcessTaskHandlerWrapper;

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
    this.taskHandler = ProcessTaskHandlerWrapper.create(
      this.descriptor,
      10 * 60 * 1000,
      48 * 60 * 60 * 1000,
    );
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessMediaFormattingTaskRequestBody,
  ): Promise<ProcessMediaFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Media formatting task for video container ${body.containerId} GCS filename ${body.gcsFilename}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessMediaFormattingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getMediaFormattingTaskMetadata(transaction, {
        mediaFormattingTaskContainerIdEq: body.containerId,
        mediaFormattingTaskGcsFilenameEq: body.gcsFilename,
      });
      if (rows.length === 0) {
        throw newBadRequestError(`Task is not found.`);
      }
      let task = rows[0];
      let delayedExecutionTime =
        this.getNow() +
        this.taskHandler.getBackoffTime(task.mediaFormattingTaskRetryCount);
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedExecutionTime}.`,
      );
      await transaction.batchUpdate([
        updateMediaFormattingTaskMetadataStatement({
          mediaFormattingTaskContainerIdEq: body.containerId,
          mediaFormattingTaskGcsFilenameEq: body.gcsFilename,
          setRetryCount: task.mediaFormattingTaskRetryCount + 1,
          setExecutionTimeMs: delayedExecutionTime,
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessMediaFormattingTaskRequestBody,
  ): Promise<void> {
    let { videoContainerData } = await this.getValidVideoContainerData(
      this.database,
      body.containerId,
      body.gcsFilename,
    );
    let r2RootDirname = videoContainerData.r2RootDirname;
    let tempDir = `${MEDIA_TEMP_DIR}/${body.gcsFilename}/${this.generateUuid()}`;
    try {
      await this.startProcessing(
        loggingPrefix,
        body.containerId,
        body.gcsFilename,
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
          ProcessMediaFormattingTaskHandler.INTERVAL_TO_DELAY_RETRY_MS,
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
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`,
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
      let { videoContainerData } = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      videoContainerData.processing = undefined;
      videoContainerData.lastProcessingFailures = failures;
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: containerId,
          setData: videoContainerData,
        }),
        deleteMediaFormattingTaskStatement({
          mediaFormattingTaskContainerIdEq: containerId,
          mediaFormattingTaskGcsFilenameEq: gcsFilename,
        }),
        insertGcsFileDeletingTaskStatement({
          filename: gcsFilename,
          uploadSessionUrl: "",
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
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
          insertR2KeyStatement({ key: `${r2RootDir}/${videoDir}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${videoDir}`,
            retryCount: 0,
            executionTimeMs: delayedTime,
            createdTimeMs: now,
          }),
        );
      }
      for (let audioDir of audioDirs) {
        statements.push(
          insertR2KeyStatement({ key: `${r2RootDir}/${audioDir}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${audioDir}`,
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

  private async delayExecutionTime(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getMediaFormattingTaskMetadata(transaction, {
        mediaFormattingTaskContainerIdEq: containerId,
        mediaFormattingTaskGcsFilenameEq: gcsFilename,
      });
      if (rows.length === 0) {
        throw newConflictError(
          `When delaying task further, task is not found.`,
        );
      }
      let task = rows[0];
      let delayedExecutionTime =
        task.mediaFormattingTaskExecutionTimeMs +
        ProcessMediaFormattingTaskHandler.INTERVAL_TO_DELAY_RETRY_MS;
      console.log(
        `${loggingPrefix} Task still not finished yet. Delaying all tasks execution time to ${delayedExecutionTime}.`,
      );
      await transaction.batchUpdate([
        updateMediaFormattingTaskMetadataStatement({
          mediaFormattingTaskContainerIdEq: containerId,
          mediaFormattingTaskGcsFilenameEq: gcsFilename,
          setRetryCount: task.mediaFormattingTaskRetryCount,
          setExecutionTimeMs: delayedExecutionTime,
        }),
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
      `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`,
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
          ENV_VARS.r2VideoBucketName,
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
          ENV_VARS.r2VideoBucketName,
          `${r2RootDirname}/${audioDir}`,
        ).start(),
      );
    }
    await spawnAsync(
      `${loggingPrefix} When formatting video to HLS:`,
      "ffmpeg",
      formattingArgs,
    );

    let videoDirAndSizeOptional = new Array<DirAndSize>(
      videoDirUploaderOptional.length,
    );
    let audioDirsAndSizes = new Array<DirAndSize>(audioDirUploaders.length);
    await Promise.all([
      ...videoDirUploaderOptional.map(async (videoDirUploader, i) => {
        let totalBytes = await videoDirUploader.flush();
        videoDirAndSizeOptional[i] = {
          dirname: videoDirOptional[i],
          totalBytes,
        };
      }),
      ...audioDirUploaders.map(async (audioDirUploader, i) => {
        let totalBytes = await audioDirUploader.flush();
        audioDirsAndSizes[i] = {
          dirname: audioDirs[i],
          totalBytes,
        };
      }),
    ]);
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
      let { videoContainerAccountId, videoContainerData } =
        await this.getValidVideoContainerData(
          transaction,
          containerId,
          gcsFilename,
        );
      videoContainerData.processing = undefined; // Processing completed.
      videoDirAndSizeOptional.forEach((videoDirAndSize) => {
        videoContainerData.videoTracks.push({
          r2TrackDirname: videoDirAndSize.dirname,
          totalBytes: videoDirAndSize.totalBytes,
          durationSec: videoInfo.durationSec,
          resolution: videoInfo.resolution,
          staging: {
            toAdd: true,
          },
        });
      });
      let existingAudios = videoContainerData.audioTracks.length;
      audioDirsAndSizes.forEach((audioDirAndSize, index) => {
        videoContainerData.audioTracks.push({
          r2TrackDirname: audioDirAndSize.dirname,
          totalBytes: audioDirAndSize.totalBytes,
          staging: {
            toAdd: {
              name: `${existingAudios + index + 1}`,
              isDefault: existingAudios + index === 0, // If this is the first audio track
            },
          },
        });
      });

      let now = this.getNow();
      // TODO: Add a task to send notification to users when completed.
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: containerId,
          setData: videoContainerData,
        }),
        deleteMediaFormattingTaskStatement({
          mediaFormattingTaskContainerIdEq: containerId,
          mediaFormattingTaskGcsFilenameEq: gcsFilename,
        }),
        insertGcsFileDeletingTaskStatement({
          filename: gcsFilename,
          uploadSessionUrl: "",
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        ...videoDirAndSizeOptional.map((videoDirAndSize) =>
          insertStorageStartRecordingTaskStatement({
            r2Dirname: `${r2RootDirname}/${videoDirAndSize.dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              totalBytes: videoDirAndSize.totalBytes,
              startTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...audioDirsAndSizes.map((audioDirAndSize) =>
          insertStorageStartRecordingTaskStatement({
            r2Dirname: `${r2RootDirname}/${audioDirAndSize.dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              totalBytes: audioDirAndSize.totalBytes,
              startTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...videoDirAndSizeOptional.map((videoDirAndSize) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${videoDirAndSize.dirname}`,
          }),
        ),
        ...audioDirsAndSizes.map((audioDirAndSize) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${audioDirAndSize.dirname}`,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async getValidVideoContainerData(
    transaction: Database | Transaction,
    containerId: string,
    gcsFilename: string,
  ): Promise<GetVideoContainerRow> {
    let videoContainerRows = await getVideoContainer(transaction, {
      videoContainerContainerIdEq: containerId,
    });
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Video container ${containerId} is not found.`);
    }
    let videoContainer = videoContainerRows[0];
    if (!videoContainer.videoContainerData.processing?.media?.formatting) {
      throw newConflictError(
        `Video container ${containerId} is not in media formatting state.`,
      );
    }
    if (
      videoContainer.videoContainerData.processing.media.formatting
        .gcsFilename !== gcsFilename
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
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${videoDir}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${audioDir}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }
}
