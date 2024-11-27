import crypto = require("crypto");
import {
  GCS_VIDEO_LOCAL_DIR,
  R2_REMOTE_CONFIG_NAME,
  R2_VIDEO_LOCAL_DIR,
  R2_VIDEO_REMOTE_BUCKET,
} from "../common/env_vars";
import { AUDIO_TRACKS_LIMIT, HLS_SEGMENT_TIME } from "../common/params";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import { VideoContainerData } from "../db/schema";
import {
  delayMediaFormattingTaskStatement,
  delayR2KeyCleanupTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyCleanupTaskStatement,
  getVideoContainer,
  insertGcsFileCleanupTaskStatement,
  insertR2KeyCleanupTaskStatement,
  insertR2KeyStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { FailureReason } from "../failure_reason";
import {
  ProcessMediaFormattingTaskRequestBody,
  ProcessMediaFormattingTaskResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import {
  Statement,
  Transaction,
} from "@google-cloud/spanner/build/src/transaction";
import { BlockingLoop } from "@selfage/blocking_loop";
import { newConflictError } from "@selfage/http_error";

export interface VideoInfo {
  durationSec: number;
  resolution: string;
}

export interface DirAndSize {
  dirname: string;
  totalBytes: number;
}

export class ProcessMediaFormattingTaskHandler {
  public static create(): ProcessMediaFormattingTaskHandler {
    return new ProcessMediaFormattingTaskHandler(
      SPANNER_DATABASE,
      BlockingLoop.createWithTimeout(),
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static TIME_TO_DELAY_EXECUTION_MS = 8 * 60 * 1000;
  private static INTERVAL_TO_DELAY_EXECUTION_MS = 5 * 60 * 1000;
  private static VIDEO_CODEC = "h264";
  private static AUDIO_CODEC = "aac";
  private static GET_DISK_USAGE_REGEX = /^(\d+)[\s\S]*$/;

  private doneCallback: () => void = () => {};
  private interfereFormat: () => Promise<void> = () => Promise.resolve();

  public constructor(
    private database: Database,
    private blockingLoop: BlockingLoop,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {}

  public setDoneCallback(doneCallback: () => void): void {
    this.doneCallback = doneCallback;
  }

  public setInterfereFormat(interfereFormat: () => Promise<void>): void {
    this.interfereFormat = interfereFormat;
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessMediaFormattingTaskRequestBody,
  ): Promise<ProcessMediaFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Media formatting task for container ${body.containerId} GCS filename ${body.gcsFilename}:`;
    let { r2ContainerDir, existingAudios } =
      await this.getPayloadAndDelayExecutionTime(
        loggingPrefix,
        body.containerId,
        body.gcsFilename,
      );
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.containerId,
      body.gcsFilename,
      r2ContainerDir,
      existingAudios,
    );
    return {};
  }

  private async getPayloadAndDelayExecutionTime(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
  ): Promise<{
    r2ContainerDir: string;
    existingAudios: number;
  }> {
    let r2ContainerDir: string;
    let existingAudios: number;
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      r2ContainerDir = videoContainer.r2Dirname;
      existingAudios = this.countAudioTracks(videoContainer);
      let delayedTime =
        this.getNow() +
        ProcessMediaFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        delayMediaFormattingTaskStatement(
          delayedTime,
          containerId,
          gcsFilename,
        ),
      ]);
      await transaction.commit();
    });
    return {
      r2ContainerDir,
      existingAudios,
    };
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2ContainerDir: string,
    existingAudios: number,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task starting.`);
    try {
      await this.startProcessing(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2ContainerDir,
        existingAudios,
      );
      console.log(`${loggingPrefix} Task completed!`);
    } catch (e) {
      console.error(`${loggingPrefix} ${e.stack ?? e}`);
    }
    this.doneCallback();
  }

  private async startProcessing(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2ContainerDir: string,
    existingAudios: number,
  ): Promise<void> {
    let { failures, videoInfo, audios } = await this.validateAndExtract(
      loggingPrefix,
      gcsFilename,
      existingAudios,
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
      r2ContainerDir,
      videoDirOptional,
      audioDirs,
    );
    this.blockingLoop
      .setInterval(
        ProcessMediaFormattingTaskHandler.INTERVAL_TO_DELAY_EXECUTION_MS,
      )
      .setAction(() =>
        this.delayExecutionTime(
          loggingPrefix,
          containerId,
          gcsFilename,
          r2ContainerDir,
          videoDirOptional,
          audioDirs,
        ),
      )
      .start();
    let { videoDirAndSizeOptional, audioDirsAndSizes } = await this.toHlsFormat(
      loggingPrefix,
      gcsFilename,
      r2ContainerDir,
      videoDirOptional,
      audioDirs,
    );
    await this.blockingLoop.stop();
    await this.finalize(
      loggingPrefix,
      containerId,
      gcsFilename,
      r2ContainerDir,
      videoDirAndSizeOptional,
      videoInfo,
      audioDirsAndSizes,
    );
  }

  private async validateAndExtract(
    loggingPrefix: string,
    gcsFilename: string,
    existingAudios: number,
  ): Promise<{
    failures: Array<FailureReason>;
    videoInfo: VideoInfo | undefined;
    audios: number;
  }> {
    console.log(`${loggingPrefix} Validating codecs and extracting metadata.`);
    let stdout = await spawnAsync(
      `${loggingPrefix} When getting codec of a video:`,
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
    let totalAudios = existingAudios;
    let audios = 0;
    let videoDurationSec: number;
    let videoResolution: string;
    let failures = new Array<FailureReason>();
    JSON.parse(stdout)["streams"].forEach((stream: any, index: number) => {
      switch (stream["codec_type"]) {
        case "video": {
          if (videos < 1) {
            let codec = stream["codec_name"];
            if (codec !== ProcessMediaFormattingTaskHandler.VIDEO_CODEC) {
              console.log(
                `${loggingPrefix} ${index}th stream is a video with codec ${codec} but requires ${ProcessMediaFormattingTaskHandler.VIDEO_CODEC}.`,
              );
              failures.push(FailureReason.VIDEO_CODEC_REQUIRES_H264);
            }
            videoDurationSec = parseInt(stream["duration"]);
            videoResolution = `${stream["width"]}x${stream["height"]}`;
          } // Ignores other video streams.
          videos++;
          break;
        }
        case "audio": {
          if (audios < AUDIO_TRACKS_LIMIT) {
            let codec = stream["codec_name"];
            if (codec !== ProcessMediaFormattingTaskHandler.AUDIO_CODEC) {
              console.log(
                `${loggingPrefix} ${index}th stream is a audio with codec ${codec} but requires ${ProcessMediaFormattingTaskHandler.AUDIO_CODEC}.`,
              );
              failures.push(FailureReason.AUDIO_CODEC_REQUIRES_AAC);
            }
          }
          totalAudios++;
          audios++;
          break;
        }
        // Ignores other types of stream.
      }
    });
    if (totalAudios > AUDIO_TRACKS_LIMIT) {
      failures.push(FailureReason.AUDIO_TOO_MANY_TRACKS);
    }
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
    failures: Array<FailureReason>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Reporting failures: ${failures.map((failure) => FailureReason[failure]).join()}`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      videoContainer.processing = {
        lastFailures: failures,
      };
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer, containerId),
        deleteMediaFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileCleanupTaskStatement(gcsFilename, now, now),
      ]);
      await transaction.commit();
    });
  }

  private async claimR2KeyAndPrepareCleanup(
    loggingPrefix: string,
    r2ContainerDir: string,
    videoDirOptional: Array<string>,
    audioDirs: Array<string>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessMediaFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Claiming video dir [${videoDirOptional.join()}] and audio dirs [${audioDirs.join()}] and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      for (let videoDir of videoDirOptional) {
        statements.push(
          insertR2KeyStatement(`${r2ContainerDir}/${videoDir}`),
          insertR2KeyCleanupTaskStatement(
            `${r2ContainerDir}/${videoDir}`,
            delayedTime,
            now,
          ),
        );
      }
      for (let audioDir of audioDirs) {
        statements.push(
          insertR2KeyStatement(`${r2ContainerDir}/${audioDir}`),
          insertR2KeyCleanupTaskStatement(
            `${r2ContainerDir}/${audioDir}`,
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
    r2ContainerDir: string,
    videoDirOptional: Array<string>,
    audioDirs: Array<string>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessMediaFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Task not finished yet. Delaying all tasks execution time to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        delayMediaFormattingTaskStatement(
          delayedTime,
          containerId,
          gcsFilename,
        ),
        ...videoDirOptional.map((videoDir) =>
          delayR2KeyCleanupTaskStatement(
            delayedTime,
            `${r2ContainerDir}/${videoDir}`,
          ),
        ),
        ...audioDirs.map((audioDir) =>
          delayR2KeyCleanupTaskStatement(
            delayedTime,
            `${r2ContainerDir}/${audioDir}`,
          ),
        ),
      ]);
      await transaction.commit();
    });
  }

  private async toHlsFormat(
    loggingPrefix: string,
    gcsFilename: string,
    r2ContainerDir: string,
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
        `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${videoDir}/%d.ts`,
        "-master_pl_name",
        `m.m3u8`,
        `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${videoDir}/o.m3u8`,
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
        `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${audioDir}/%d.ts`,
        `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${audioDir}/o.m3u8`,
      );
    });
    for (let videoDir of videoDirOptional) {
      await spawnAsync(
        `${loggingPrefix} When creating directory ${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${videoDir}:`,
        "mkdir",
        ["-p", `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${videoDir}`],
      );
    }
    for (let audioDir of audioDirs) {
      await spawnAsync(
        `${loggingPrefix} When creating directory ${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${audioDir}:`,
        "mkdir",
        ["-p", `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${audioDir}`],
      );
    }
    await spawnAsync(
      `${loggingPrefix} When formatting video to HLS:`,
      "ffmpeg",
      formattingArgs,
    );
    await spawnAsync(
      `${loggingPrefix} When syncing container directory:`,
      "rclone",
      [
        "sync",
        `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}`,
        `${R2_REMOTE_CONFIG_NAME}:${R2_VIDEO_REMOTE_BUCKET}/${r2ContainerDir}`,
      ],
    );

    let videoDirAndSizeOptional = new Array<DirAndSize>();
    for (let videoDir of videoDirOptional) {
      let output = await spawnAsync(
        `${loggingPrefix} When getting size of HLS video in dir ${videoDir}:`,
        "du",
        ["-sb", `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${videoDir}`],
      );
      console.log(
        `${loggingPrefix} Get size of HLS video in dir ${videoDir} with output:`,
        output,
      );
      let totalBytes = parseInt(
        ProcessMediaFormattingTaskHandler.GET_DISK_USAGE_REGEX.exec(output)[1],
      );
      videoDirAndSizeOptional.push({
        dirname: videoDir,
        totalBytes,
      });
    }
    let audioDirsAndSizes = new Array<DirAndSize>();
    for (let audioDir of audioDirs) {
      let output = await spawnAsync(
        `${loggingPrefix} When getting size of HLS audio:`,
        "du",
        ["-sb", `${R2_VIDEO_LOCAL_DIR}/${r2ContainerDir}/${audioDir}`],
      );
      console.log(
        `${loggingPrefix} Get size of HLS audio in dir ${audioDir} with output:`,
        output,
      );
      let totalBytes = parseInt(
        ProcessMediaFormattingTaskHandler.GET_DISK_USAGE_REGEX.exec(output)[1],
      );
      audioDirsAndSizes.push({
        dirname: audioDir,
        totalBytes,
      });
    }
    return { videoDirAndSizeOptional, audioDirsAndSizes };
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2ContainerDir: string,
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
          toAdd: {
            r2TrackDirname: videoDirAndSize.dirname,
            totalBytes: videoDirAndSize.totalBytes,
            durationSec: videoInfo.durationSec,
            resolution: videoInfo.resolution,
          },
        });
      });
      let existingAudios = this.countAudioTracks(videoContainer);
      audioDirsAndSizes.forEach((audioDirAndSize, index) => {
        videoContainer.audioTracks.push({
          toAdd: {
            name: `${existingAudios + index + 1}`,
            isDefault: existingAudios + index === 0, // If this is the first audio track
            r2TrackDirname: audioDirAndSize.dirname,
            totalBytes: audioDirAndSize.totalBytes,
          },
        });
      });

      let now = this.getNow();
      // TODO: Add a task to send notification to users when completed.
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer, containerId),
        deleteMediaFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileCleanupTaskStatement(gcsFilename, now, now),
        ...videoDirAndSizeOptional.map((videoDirAndSize) =>
          deleteR2KeyCleanupTaskStatement(
            `${r2ContainerDir}/${videoDirAndSize.dirname}`,
          ),
        ),
        ...audioDirsAndSizes.map((audioDirAndSize) =>
          deleteR2KeyCleanupTaskStatement(
            `${r2ContainerDir}/${audioDirAndSize.dirname}`,
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
  ): Promise<VideoContainerData> {
    let videoContainerRows = await getVideoContainer(transaction, containerId);
    if (videoContainerRows.length === 0) {
      throw newConflictError(`Container ${containerId} is not found.`);
    }
    let videoContainer = videoContainerRows[0].videoContainerData;
    if (!videoContainer.processing?.media?.formatting) {
      throw newConflictError(
        `Container ${containerId} is not in media formatting state.`,
      );
    }
    if (
      videoContainer.processing.media.formatting.gcsFilename !== gcsFilename
    ) {
      throw newConflictError(
        `Container ${containerId} is formatting a different file than ${gcsFilename}.`,
      );
    }
    return videoContainer;
  }

  private countAudioTracks(videoContainer: VideoContainerData): number {
    let count = 0;
    videoContainer.audioTracks.forEach((audioTrack) => {
      if (audioTrack.data) {
        if (!audioTrack.toRemove) {
          count++;
        }
      } else {
        if (audioTrack.toAdd) {
          count++;
        }
      }
    });
    return count;
  }
}
