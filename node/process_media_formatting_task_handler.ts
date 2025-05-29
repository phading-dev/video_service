import crypto = require("crypto");
import {
  HLS_SEGMENT_TIME,
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
  MEDIA_TEMP_DIR,
} from "../common/constants";
import { DirectoryUploader } from "../common/r2_directory_uploader";
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
  insertUploadedRecordingTaskStatement,
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
import { ProcessingFailureReason } from "@phading/video_service_interface/node/last_processing_failure";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";
import { mkdir, rm } from "fs/promises";

export interface VideoDir {
  dirname?: string;
  durationSec?: number;
  resolution?: string;
  totalBytes?: number;
}

export interface AudioDir {
  dirname?: string;
  totalBytes?: number;
}

export class ProcessMediaFormattingTaskHandler extends ProcessMediaFormattingTaskHandlerInterface {
  public static create(): ProcessMediaFormattingTaskHandler {
    return new ProcessMediaFormattingTaskHandler(
      SPANNER_DATABASE,
      DirectoryUploader.create,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 365 * 24 * 60 * 60 * 1000; // 1 year
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  private static VIDEO_CODEC = "h264";
  private static AUDIO_CODEC = "aac";
  public interfereFormat: () => Promise<void> = async () => {};
  public interfereUpload: () => Promise<void> = async () => {};
  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private createDirectoryUploader: (
      loggingPrefix: string,
      localDir: string,
      remoteBucket: string,
      remoteDir: string,
    ) => DirectoryUploader,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {
    super();
    this.taskHandler = ProcessTaskHandlerWrapper.create(
      this.descriptor,
      30 * 60 * 1000,
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
    let { failures, videoDirOptional, audioDirs } =
      await this.validateAndExtractInfo(loggingPrefix, gcsFilename);
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
    videoDirOptional.forEach((videoDir) => {
      videoDir.dirname = this.generateUuid();
    });
    audioDirs.forEach((audioDir) => {
      audioDir.dirname = this.generateUuid();
    });

    let { failure } = await this.toHlsFormat(
      loggingPrefix,
      gcsFilename,
      tempDir,
      videoDirOptional,
      audioDirs,
    );
    if (failure) {
      await this.reportFailures(loggingPrefix, containerId, gcsFilename, [
        failure,
      ]);
      return;
    }

    await this.claimR2KeyAndPrepareCleanup(
      loggingPrefix,
      r2RootDir,
      videoDirOptional,
      audioDirs,
    );
    try {
      await this.uploadToR2(
        loggingPrefix,
        tempDir,
        r2RootDir,
        videoDirOptional,
        audioDirs,
      );
      await this.finalize(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDir,
        videoDirOptional,
        audioDirs,
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
    videoDirOptional: Array<VideoDir>;
    audioDirs: Array<AudioDir>;
  }> {
    console.log(`${loggingPrefix} Validating codecs and extracting metadata.`);
    let videoMetadata: any;
    try {
      videoMetadata = await spawnAsync(
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
    } catch (e) {
      console.error(e);
      return {
        failures: [ProcessingFailureReason.MEDIA_FORMAT_INVALID],
        videoDirOptional: [],
        audioDirs: [],
      };
    }
    console.log(
      `${loggingPrefix} Extracted metadata of the video located at GCS path ${gcsFilename} with output:`,
      videoMetadata,
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
    let numOfVideos = 0;
    let numOfAudios = 0;
    let videoDurationSec: number;
    let videoResolution: string;
    let failures = new Array<ProcessingFailureReason>();
    JSON.parse(videoMetadata)["streams"].forEach(
      (stream: any, index: number) => {
        switch (stream["codec_type"]) {
          case "video": {
            if (numOfVideos < 1) {
              let codec = stream["codec_name"];
              if (codec !== ProcessMediaFormattingTaskHandler.VIDEO_CODEC) {
                console.log(
                  `${loggingPrefix} ${index}th stream is a video with codec ${codec} but requires ${ProcessMediaFormattingTaskHandler.VIDEO_CODEC}.`,
                );
                failures.push(
                  ProcessingFailureReason.VIDEO_CODEC_REQUIRES_H264,
                );
              }
              videoDurationSec = parseInt(stream["duration"]);
              videoResolution = `${stream["width"]}x${stream["height"]}`;
            } // Ignores other video streams.
            numOfVideos++;
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
            numOfAudios++;
            break;
          }
          // Ignores other types of stream.
        }
      },
    );
    return {
      failures,
      videoDirOptional:
        numOfVideos > 0
          ? [{ durationSec: videoDurationSec, resolution: videoResolution }]
          : [],
      audioDirs: Array.from({ length: numOfAudios }, (): AudioDir => ({})),
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
      let now = this.getNow();
      videoContainerData.processing = undefined;
      videoContainerData.lastProcessingFailure = {
        reasons: failures,
        timeMs: now,
      };
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

  private async toHlsFormat(
    loggingPrefix: string,
    gcsFilename: string,
    tempDir: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<{
    failure?: ProcessingFailureReason;
  }> {
    console.log(`${loggingPrefix} Start HLS formatting.`);
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
        `${tempDir}/${videoDir.dirname}/%d.ts`,
        "-master_pl_name",
        `${LOCAL_MASTER_PLAYLIST_NAME}`,
        `${tempDir}/${videoDir.dirname}/${LOCAL_PLAYLIST_NAME}`,
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
        `${tempDir}/${audioDir.dirname}/%d.ts`,
        `${tempDir}/${audioDir.dirname}/${LOCAL_PLAYLIST_NAME}`,
      );
    });
    await Promise.all([
      ...videoDirOptional.map((videoDir) =>
        mkdir(`${tempDir}/${videoDir.dirname}`, {
          recursive: true,
        }),
      ),
      ...audioDirs.map((audioDir) =>
        mkdir(`${tempDir}/${audioDir.dirname}`, {
          recursive: true,
        }),
      ),
    ]);
    try {
      await this.interfereFormat();
      await spawnAsync(
        `${loggingPrefix} When formatting video to HLS:`,
        "ffmpeg",
        formattingArgs,
      );
    } catch (e) {
      console.error(e);
      return {
        failure: ProcessingFailureReason.MEDIA_FORMAT_FAILURE,
      };
    }
    return {};
  }

  private async claimR2KeyAndPrepareCleanup(
    loggingPrefix: string,
    r2RootDir: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming video dir [${videoDirOptional.map((dir) => dir.dirname).join()}] and audio dirs [${audioDirs.map((dir) => dir.dirname).join()}] and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      for (let videoDir of videoDirOptional) {
        statements.push(
          insertR2KeyStatement({ key: `${r2RootDir}/${videoDir.dirname}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${videoDir.dirname}`,
            retryCount: 0,
            executionTimeMs: delayedTime,
            createdTimeMs: now,
          }),
        );
      }
      for (let audioDir of audioDirs) {
        statements.push(
          insertR2KeyStatement({ key: `${r2RootDir}/${audioDir.dirname}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDir}/${audioDir.dirname}`,
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

  private async uploadToR2(
    loggingPrefix: string,
    tempDir: string,
    r2RootDirname: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    await this.interfereUpload();
    await Promise.all([
      ...videoDirOptional.map(async (videoDir) => {
        videoDir.totalBytes = await this.createDirectoryUploader(
          loggingPrefix,
          `${tempDir}/${videoDir.dirname}`,
          ENV_VARS.r2VideoBucketName,
          `${r2RootDirname}/${videoDir.dirname}`,
        ).upload();
      }),
      ...audioDirs.map(async (audioDir) => {
        audioDir.totalBytes = await this.createDirectoryUploader(
          loggingPrefix,
          `${tempDir}/${audioDir.dirname}`,
          ENV_VARS.r2VideoBucketName,
          `${r2RootDirname}/${audioDir.dirname}`,
        ).upload();
      }),
    ]);
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDirname: string,
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
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
      videoDirOptional.forEach((videoDir) => {
        videoContainerData.videoTracks.push({
          r2TrackDirname: videoDir.dirname,
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
          r2TrackDirname: audioDir.dirname,
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
        insertUploadedRecordingTaskStatement({
          gcsFilename,
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
            r2Dirname: `${r2RootDirname}/${videoDir.dirname}`,
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
            r2Dirname: `${r2RootDirname}/${audioDir.dirname}`,
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
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${videoDir.dirname}`,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${audioDir.dirname}`,
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
    if (!videoContainer.videoContainerData.processing?.mediaFormatting) {
      throw newConflictError(
        `Video container ${containerId} is not in media formatting state.`,
      );
    }
    if (
      videoContainer.videoContainerData.processing.mediaFormatting
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
    videoDirOptional: Array<VideoDir>,
    audioDirs: Array<AudioDir>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up video dir [${videoDirOptional.map((dir) => dir.dirname).join()}] and audio dirs [${audioDirs.map((dir) => dir.dirname).join()}] in ${ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate([
        ...videoDirOptional.map((videoDir) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${videoDir.dirname}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
        ...audioDirs.map((audioDir) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${audioDir.dirname}`,
            setRetryCount: 0,
            setExecutionTimeMs: delayedTime,
          }),
        ),
      ]);
      await transaction.commit();
    });
  }
}
