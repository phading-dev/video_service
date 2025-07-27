import crypto = require("crypto");
import {
  GCS_OUTPUT_PREFIX,
  HLS_SEGMENT_TIME,
  LOCAL_MASTER_PLAYLIST_NAME,
  LOCAL_PLAYLIST_NAME,
} from "../common/constants";
import { GcsDirWritesVerifier } from "../common/gcs_dir_writes_verifier";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import {
  GetVideoContainerRow,
  deleteGcsKeyDeletingTaskStatement,
  deleteMediaFormattingTaskStatement,
  getMediaFormattingTaskMetadata,
  getVideoContainer,
  insertGcsKeyDeletingTaskStatement,
  insertGcsKeyStatement,
  insertMediaUploadingTaskStatement,
  updateGcsKeyDeletingTaskMetadataStatement,
  updateMediaFormattingTaskMetadataStatement,
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
import { mkdir } from "fs/promises";

interface VideoInfo {
  gcsDirname?: string;
  durationSec?: number;
  resolution?: string;
}

export class ProcessMediaFormattingTaskHandler extends ProcessMediaFormattingTaskHandlerInterface {
  public static create(): ProcessMediaFormattingTaskHandler {
    return new ProcessMediaFormattingTaskHandler(
      GcsDirWritesVerifier.create,
      SPANNER_DATABASE,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  private static VIDEO_CODEC = "h264";
  private static AUDIO_CODEC = "aac";
  public interfereFormat: () => Promise<void> = async () => {};
  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private newGcsDirWritesVerifier: typeof GcsDirWritesVerifier.create,
    private database: Database,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {
    super();
    this.taskHandler = ProcessTaskHandlerWrapper.create(
      this.descriptor,
      60 * 60 * 1000,
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
    await this.getValidVideoContainerData(
      this.database,
      body.containerId,
      body.gcsFilename,
    );

    let { failures, videoInfoOptional, numOfAudios } =
      await this.validateAndExtractInfo(loggingPrefix, body.gcsFilename);
    if (failures.length > 0) {
      await this.reportFailures(
        loggingPrefix,
        body.containerId,
        body.gcsFilename,
        failures,
      );
      return;
    }

    // Create new dirs so that if same task got executed twice, they won't overwrite each other.
    let gcsOutputDir = `${GCS_OUTPUT_PREFIX}${this.generateUuid()}`;
    let audioDirs: Array<string> = [];
    for (let videoInfo of videoInfoOptional) {
      videoInfo.gcsDirname = this.generateUuid();
    }
    for (let i = 0; i < numOfAudios; i++) {
      audioDirs.push(this.generateUuid());
    }

    await this.claimGcsOutputDirAndPrepareCleanup(loggingPrefix, gcsOutputDir);
    try {
      let { failure } = await this.toHlsFormatAndUpload(
        loggingPrefix,
        body.gcsFilename,
        gcsOutputDir,
        videoInfoOptional,
        audioDirs,
      );
      if (failure) {
        await this.reportFailures(
          loggingPrefix,
          body.containerId,
          body.gcsFilename,
          [failure],
          gcsOutputDir,
        );
        return;
      }
      await this.finalize(
        loggingPrefix,
        body.containerId,
        body.gcsFilename,
        gcsOutputDir,
        videoInfoOptional,
        audioDirs,
      );
    } catch (e) {
      await this.cleanupGcsOutputDirOnError(loggingPrefix, gcsOutputDir);
      throw e;
    }
  }

  private async validateAndExtractInfo(
    loggingPrefix: string,
    gcsFilename: string,
  ): Promise<{
    failures: Array<ProcessingFailureReason>;
    videoInfoOptional?: Array<VideoInfo>;
    numOfAudios?: number;
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
          "stream=codec_name,codec_type,width,height",
          "-of",
          "json",
          `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`,
        ],
      );
    } catch (e) {
      console.error(e);
      return {
        failures: [ProcessingFailureReason.MEDIA_FORMAT_INVALID],
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

    let durationSec: number;
    try {
      let durationStr = await spawnAsync(
        `${loggingPrefix} When getting codec of the video container file:`,
        "ffprobe",
        [
          "-v",
          "error",
          "-show_entries",
          "format=duration",
          "-of",
          "default=noprint_wrappers=1:nokey=1",
          `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`,
        ],
      );
      durationSec = parseInt(durationStr.trim());
      if (isNaN(durationSec) || durationSec <= 0) {
        throw new Error(`Invalid duration: ${durationStr}`);
      }
    } catch (e) {
      console.error(e);
      return {
        failures: [ProcessingFailureReason.MEDIA_FORMAT_INVALID],
      };
    }
    console.log(
      `${loggingPrefix} Extracted duration of the video located at GCS path ${gcsFilename} with output:`,
      durationSec,
    );
    return {
      failures,
      videoInfoOptional:
        numOfVideos > 0 ? [{ durationSec, resolution: videoResolution }] : [],
      numOfAudios,
    };
  }

  private async reportFailures(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    failures: Array<ProcessingFailureReason>,
    gcsOutputDir?: string,
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
        insertGcsKeyDeletingTaskStatement({
          key: gcsFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        ...(gcsOutputDir
          ? [
              updateGcsKeyDeletingTaskMetadataStatement({
                gcsKeyDeletingTaskKeyEq: gcsOutputDir,
                setRetryCount: 0,
                setExecutionTimeMs: now,
              }),
            ]
          : []),
      ]);
      await transaction.commit();
    });
  }

  private async claimGcsOutputDirAndPrepareCleanup(
    loggingPrefix: string,
    gcsOutputDir: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming output dir ${gcsOutputDir} and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      statements.push(
        insertGcsKeyStatement({ key: gcsOutputDir }),
        insertGcsKeyDeletingTaskStatement({
          key: gcsOutputDir,
          retryCount: 0,
          executionTimeMs: delayedTime,
          createdTimeMs: now,
        }),
      );
      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
  }

  private async toHlsFormatAndUpload(
    loggingPrefix: string,
    gcsInputFilename: string,
    gcsOutputDir: string,
    videoInfoOptional: Array<VideoInfo>,
    audioDirs: Array<string>,
  ): Promise<{
    failure?: ProcessingFailureReason;
  }> {
    console.log(`${loggingPrefix} Start HLS formatting.`);
    await Promise.all([
      ...videoInfoOptional.map((videoInfo) =>
        mkdir(
          `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${videoInfo.gcsDirname}`,
          {
            recursive: true,
          },
        ),
      ),
      ...audioDirs.map((audioDir) =>
        mkdir(
          `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${audioDir}`,
          {
            recursive: true,
          },
        ),
      ),
    ]);

    let formattingArgs: Array<string> = [
      "-n",
      "19",
      "ffmpeg",
      "-i",
      `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsInputFilename}`,
      "-loglevel",
      "error",
    ];
    videoInfoOptional.forEach((videoInfo) => {
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
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${videoInfo.gcsDirname}/%d.ts`,
        "-master_pl_name",
        `${LOCAL_MASTER_PLAYLIST_NAME}`,
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${videoInfo.gcsDirname}/${LOCAL_PLAYLIST_NAME}`,
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
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${audioDir}/%d.ts`,
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsOutputDir}/${audioDir}/${LOCAL_PLAYLIST_NAME}`,
      );
    });
    let failure: ProcessingFailureReason;
    try {
      await this.interfereFormat();
      await spawnAsync(
        `${loggingPrefix} When formatting video to HLS:`,
        "nice",
        formattingArgs,
      );
    } catch (e) {
      console.error(e);
      failure = ProcessingFailureReason.MEDIA_FORMAT_FAILURE;
    }
    console.log(`${loggingPrefix} Verifying GCS dir writes.`);
    if (videoInfoOptional.length > 0) {
      await this.newGcsDirWritesVerifier(
        ENV_VARS.gcsVideoMountedLocalDir,
        ENV_VARS.gcsVideoBucketName,
        `${gcsOutputDir}/${videoInfoOptional[0].gcsDirname}`,
      );
    }
    for (let audioDir of audioDirs) {
      await this.newGcsDirWritesVerifier(
        ENV_VARS.gcsVideoMountedLocalDir,
        ENV_VARS.gcsVideoBucketName,
        `${gcsOutputDir}/${audioDir}`,
      );
    }
    if (failure) {
      return { failure };
    } else {
      return {};
    }
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsInputFilename: string,
    gcsOutputDir: string,
    videoInfoOptional: Array<VideoInfo>,
    audioDirs: Array<string>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let { videoContainerData } = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsInputFilename,
      );
      videoContainerData.processing = {
        mediaUploading: {
          gcsDirname: gcsOutputDir,
          gcsVideoDirname: videoInfoOptional[0]
            ? videoInfoOptional[0].gcsDirname
            : undefined,
          videoInfo: videoInfoOptional[0]
            ? {
                durationSec: videoInfoOptional[0].durationSec,
                resolution: videoInfoOptional[0].resolution,
              }
            : undefined,
          gcsAudioDirnames: audioDirs,
        },
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: containerId,
          setData: videoContainerData,
        }),
        deleteMediaFormattingTaskStatement({
          mediaFormattingTaskContainerIdEq: containerId,
          mediaFormattingTaskGcsFilenameEq: gcsInputFilename,
        }),
        insertMediaUploadingTaskStatement({
          containerId,
          gcsDirname: gcsOutputDir,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        insertGcsKeyDeletingTaskStatement({
          key: gcsInputFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        deleteGcsKeyDeletingTaskStatement({
          gcsKeyDeletingTaskKeyEq: gcsOutputDir,
        }),
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

  private async cleanupGcsOutputDirOnError(
    loggingPrefix: string,
    gcsOutputDir: string,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up output dir ${gcsOutputDir} in ${ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessMediaFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate([
        updateGcsKeyDeletingTaskMetadataStatement({
          gcsKeyDeletingTaskKeyEq: gcsOutputDir,
          setRetryCount: 0,
          setExecutionTimeMs: delayedTime,
        }),
      ]);
      await transaction.commit();
    });
  }
}
