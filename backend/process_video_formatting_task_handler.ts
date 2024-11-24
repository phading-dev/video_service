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
import {
  delayR2KeyCleanupTaskStatement,
  delayVideoFormattingTaskStatement,
  deleteR2KeyCleanupTaskStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoFormattingTaskStatement,
  deleteVideoTrackStatement,
  getAllAudioTracks,
  getAllVideoTracks,
  getVideoContainer,
  getVideoTrack,
  insertAudioTrackStatement,
  insertGcsFileCleanupTaskStatement,
  insertR2KeyCleanupTaskStatement,
  insertR2KeyStatement,
  insertVideoContainerSyncingTaskStatement,
  updateVideoContainerStatement,
  updateVideoTrackStatement,
} from "../db/sql";
import { FailureReason } from "../failure_reason";
import {
  ProcessVideoFormattingTaskRequestBody,
  ProcessVideoFormattingTaskResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { BlockingLoop } from "@selfage/blocking_loop";
import {
  newConflictError,
  newInternalServerErrorError,
  newNotFoundError,
} from "@selfage/http_error";

export interface DirAndSize {
  dirname: string;
  totalBytes: number;
}

export class ProcessVideoFormattingTaskHandler {
  public static create(): ProcessVideoFormattingTaskHandler {
    return new ProcessVideoFormattingTaskHandler(
      SPANNER_DATABASE,
      GCS_VIDEO_LOCAL_DIR,
      R2_VIDEO_LOCAL_DIR,
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
    private gcsVideoBucketDirName: string,
    private r2VideoBucketDirName: string,
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
    body: ProcessVideoFormattingTaskRequestBody,
  ): Promise<ProcessVideoFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Video formatting task for container ${body.containerId} video ${body.videoId}:`;
    let { r2ContainerDir, gcsFilename, existingAudios } =
      await this.getPayloadAndDelayExecutionTime(loggingPrefix, body);
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.containerId,
      body.videoId,
      r2ContainerDir,
      gcsFilename,
      existingAudios,
    );
    return {};
  }

  private async getPayloadAndDelayExecutionTime(
    loggingPrefix: string,
    body: ProcessVideoFormattingTaskRequestBody,
  ): Promise<{
    gcsFilename: string;
    r2ContainerDir: string;
    existingAudios: number;
  }> {
    let r2ContainerDir: string;
    let gcsFilename: string;
    let existingAudios: number;
    await this.database.runTransactionAsync(async (transaction) => {
      let [videoContainerRows, videoTrackRows, audioTrackRows] =
        await Promise.all([
          getVideoContainer(transaction, body.containerId),
          getVideoTrack(transaction, body.containerId, body.videoId),
          getAllAudioTracks(transaction, body.containerId),
        ]);
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(`Container ${body.containerId} is not found.`);
      }
      if (videoTrackRows.length === 0) {
        throw newNotFoundError(
          `Container ${body.containerId} video ${body.videoId} is not found.`,
        );
      }
      if (!videoTrackRows[0].videoTrackData.formatting) {
        throw newNotFoundError(
          `Container ${body.containerId} video ${body.videoId} is not in formatting state.`,
        );
      }
      r2ContainerDir = videoContainerRows[0].videoContainerData.r2Dirname;
      gcsFilename = videoTrackRows[0].videoTrackData.formatting.gcsFilename;
      existingAudios = audioTrackRows.length;
      let delayedTime =
        this.getNow() +
        ProcessVideoFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        delayVideoFormattingTaskStatement(
          delayedTime,
          body.containerId,
          body.videoId,
        ),
      ]);
      await transaction.commit();
    });
    return {
      r2ContainerDir,
      gcsFilename,
      existingAudios,
    };
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    containerId: string,
    videoId: string,
    r2ContainerDir: string,
    gcsFilename: string,
    existingAudios: number,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task starting.`);
    try {
      await this.startProcessing(
        loggingPrefix,
        containerId,
        videoId,
        r2ContainerDir,
        gcsFilename,
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
    videoId: string,
    r2ContainerDir: string,
    gcsFilename: string,
    existingAudios: number,
  ): Promise<void> {
    let { failures, audios, videoDurationSec, videoResolution } =
      await this.validateAndExtract(loggingPrefix, gcsFilename, existingAudios);
    if (failures.length > 0) {
      await this.reportFailures(
        loggingPrefix,
        failures,
        containerId,
        videoId,
        gcsFilename,
      );
      return;
    }

    // Create new R2 dirs so that if same task got executed twice, they won't overwrite each other.
    let videoDir = this.generateUuid();
    let audioDirs = new Array<string>();
    for (let i = 0; i < audios; i++) {
      audioDirs.push(this.generateUuid());
    }
    await this.claimR2KeyAndPrepareCleanup(
      loggingPrefix,
      r2ContainerDir,
      videoDir,
      audioDirs,
    );
    this.blockingLoop
      .setInterval(
        ProcessVideoFormattingTaskHandler.INTERVAL_TO_DELAY_EXECUTION_MS,
      )
      .setAction(() =>
        this.delayExecutionTime(
          loggingPrefix,
          containerId,
          videoId,
          r2ContainerDir,
          videoDir,
          audioDirs,
        ),
      )
      .start();
    let { videoDirAndSize, audioDirsAndSizes } = await this.toHlsFormat(
      loggingPrefix,
      gcsFilename,
      r2ContainerDir,
      videoDir,
      audioDirs,
    );
    await this.blockingLoop.stop();
    await this.finalize(
      loggingPrefix,
      containerId,
      videoId,
      gcsFilename,
      r2ContainerDir,
      videoDirAndSize,
      videoResolution,
      videoDurationSec,
      audioDirsAndSizes,
    );
  }

  private async validateAndExtract(
    loggingPrefix: string,
    gcsFilename: string,
    existingAudios: number,
  ): Promise<{
    failures: Array<FailureReason>;
    audios: number;
    videoResolution: string;
    videoDurationSec: number;
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
        `${this.gcsVideoBucketDirName}/${gcsFilename}`,
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
            if (codec !== ProcessVideoFormattingTaskHandler.VIDEO_CODEC) {
              console.log(
                `${loggingPrefix} ${index}th stream is a video with codec ${codec} but requires ${ProcessVideoFormattingTaskHandler.VIDEO_CODEC}.`,
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
            if (codec !== ProcessVideoFormattingTaskHandler.AUDIO_CODEC) {
              console.log(
                `${loggingPrefix} ${index}th stream is a audio with codec ${codec} but requires ${ProcessVideoFormattingTaskHandler.AUDIO_CODEC}.`,
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
    if (videos === 0) {
      failures.push(FailureReason.VIDEO_NEEDS_AT_LEAST_ONE_TRACK);
    }
    if (totalAudios > AUDIO_TRACKS_LIMIT) {
      failures.push(FailureReason.AUDIO_TOO_MANY_TRACKS);
    }
    return {
      failures,
      audios,
      videoDurationSec,
      videoResolution,
    };
  }

  private async reportFailures(
    loggingPrefix: string,
    failures: Array<FailureReason>,
    containerId: string,
    videoId: string,
    gcsFilename: string,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Reporting failures: ${failures.map((failure) => FailureReason[failure]).join()}`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoTrackStatement(
          {
            failure: {
              reasons: failures,
            },
          },
          containerId,
          videoId,
        ),
        deleteVideoFormattingTaskStatement(containerId, videoId),
        insertGcsFileCleanupTaskStatement(gcsFilename, now, now),
      ]);
      await transaction.commit();
    });
  }

  private async claimR2KeyAndPrepareCleanup(
    loggingPrefix: string,
    r2ContainerDir: string,
    videoDir: string,
    audioDirs: Array<string>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessVideoFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Claiming video dir ${videoDir} and audio dirs ${audioDirs.join()} and set to clean up at ${delayedTime}.`,
      );
      let statements: Array<Statement> = [
        insertR2KeyStatement(`${r2ContainerDir}/${videoDir}`),
        insertR2KeyCleanupTaskStatement(
          `${r2ContainerDir}/${videoDir}`,
          delayedTime,
          now,
        ),
      ];
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
    videoId: string,
    r2ContainerDir: string,
    videoDir: string,
    audioDirs: Array<string>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessVideoFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS;
      console.log(
        `${loggingPrefix} Task not finished yet. Delaying all tasks execution time to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        delayVideoFormattingTaskStatement(delayedTime, containerId, videoId),
        delayR2KeyCleanupTaskStatement(
          delayedTime,
          `${r2ContainerDir}/${videoDir}`,
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
    videoDir: string,
    audioDirs: Array<string>,
  ): Promise<{
    videoDirAndSize: DirAndSize;
    audioDirsAndSizes: Array<DirAndSize>;
  }> {
    console.log(`${loggingPrefix} Start HLS formatting.`);
    await this.interfereFormat();
    let formattingArgs = new Array<string>();
    formattingArgs.push(
      "-i",
      `${this.gcsVideoBucketDirName}/${gcsFilename}`,
      "-loglevel",
      "error",
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
      `${this.r2VideoBucketDirName}/${r2ContainerDir}/${videoDir}/%d.ts`,
      "-master_pl_name",
      `m.m3u8`,
      `${this.r2VideoBucketDirName}/${r2ContainerDir}/${videoDir}/o.m3u8`,
    );
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
        `${this.r2VideoBucketDirName}/${r2ContainerDir}/${audioDir}/%d.ts`,
        `${this.r2VideoBucketDirName}/${r2ContainerDir}/${audioDir}/o.m3u8`,
      );
    });
    await spawnAsync(
      `${loggingPrefix} When creating directory ${this.r2VideoBucketDirName}/${r2ContainerDir}/${videoDir}:`,
      "mkdir",
      ["-p", `${this.r2VideoBucketDirName}/${r2ContainerDir}/${videoDir}`],
    );
    for (let audioDir of audioDirs) {
      await spawnAsync(
        `${loggingPrefix} When creating directory ${this.r2VideoBucketDirName}/${r2ContainerDir}/${audioDir}:`,
        "mkdir",
        ["-p", `${this.r2VideoBucketDirName}/${r2ContainerDir}/${audioDir}`],
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
        `${this.r2VideoBucketDirName}/${r2ContainerDir}`,
        `${R2_REMOTE_CONFIG_NAME}:${R2_VIDEO_REMOTE_BUCKET}/${r2ContainerDir}`,
      ],
    );

    let output = await spawnAsync(
      `${loggingPrefix} When getting size of HLS video in dir ${videoDir}:`,
      "du",
      ["-sb", `${this.r2VideoBucketDirName}/${r2ContainerDir}/${videoDir}`],
    );
    console.log(
      `${loggingPrefix} Get size of HLS video in dir ${videoDir} with output:`,
      output,
    );
    let videoTotalBytes = parseInt(
      ProcessVideoFormattingTaskHandler.GET_DISK_USAGE_REGEX.exec(output)[1],
    );
    let videoDirAndSize: DirAndSize = {
      dirname: videoDir,
      totalBytes: videoTotalBytes,
    };
    let audioDirsAndSizes = new Array<DirAndSize>();
    for (let audioDir of audioDirs) {
      let output = await spawnAsync(
        `${loggingPrefix} When getting size of HLS audio:`,
        "du",
        ["-sb", `${this.r2VideoBucketDirName}/${r2ContainerDir}/${audioDir}`],
      );
      console.log(
        `${loggingPrefix} Get size of HLS audio in dir ${audioDir} with output:`,
        output,
      );
      let totalBytes = parseInt(
        ProcessVideoFormattingTaskHandler.GET_DISK_USAGE_REGEX.exec(output)[1],
      );
      audioDirsAndSizes.push({
        dirname: audioDir,
        totalBytes,
      });
    }
    return { videoDirAndSize, audioDirsAndSizes };
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    videoId: string,
    gcsFilename: string,
    r2ContainerDir: string,
    videoDirAndSize: DirAndSize,
    videoResolution: string,
    videoDurationSec: number,
    audioDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let [videoContainerRows, videoTrackRows, audioTrackRows] =
        await Promise.all([
          getVideoContainer(transaction, containerId),
          getAllVideoTracks(transaction, containerId),
          getAllAudioTracks(transaction, containerId),
        ]);
      if (videoContainerRows.length === 0) {
        throw newConflictError(`Container ${containerId} is not found.`);
      }
      if (videoTrackRows.length === 0) {
        throw newConflictError(`Container ${containerId} has no video tracks.`);
      }
      for (let videoTrackRow of videoTrackRows) {
        if (videoTrackRow.videoTrackVideoId === videoId) {
          if (!videoTrackRow.videoTrackData.formatting) {
            // Validate first before any loggings below.
            throw newConflictError(
              `Container ${containerId} video ${videoId} is not in formatting state.`,
            );
          }
          if (
            videoTrackRow.videoTrackData.formatting.gcsFilename !== gcsFilename
          ) {
            // gcsFilename should be immutable.
            throw newInternalServerErrorError(
              `${loggingPrefix} Container ${containerId} video ${videoId} gcsFilename has changed from ${gcsFilename} to ${videoTrackRow.videoTrackData.formatting.gcsFilename}.`,
            );
          }
        }
      }

      let containerData = videoContainerRows[0].videoContainerData;
      let oldVersion = containerData.version++;
      containerData.totalBytes += videoDirAndSize.totalBytes;
      audioDirsAndSizes.forEach(
        (audioDirAndSize) =>
          (containerData.totalBytes += audioDirAndSize.totalBytes),
      );

      let videoIdsToDelete = new Array<string>();
      let r2VideoDirsToDelete = new Array<string>();
      for (let videoTrackRow of videoTrackRows) {
        if (videoTrackRow.videoTrackVideoId !== videoId) {
          videoIdsToDelete.push(videoTrackRow.videoTrackVideoId);
          if (videoTrackRow.videoTrackData.done) {
            r2VideoDirsToDelete.push(
              videoTrackRow.videoTrackData.done.r2TrackDirname,
            );
            containerData.totalBytes -=
              videoTrackRow.videoTrackData.done.totalBytes;
          } else {
            console.warn(
              `Container ${containerId} video ${videoTrackRow.videoTrackVideoId} is not in done state when video ${videoId} is formatting.`,
            );
          }
        }
      }
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(containerData, containerId),
        deleteVideoContainerSyncingTaskStatement(containerId, oldVersion),
        insertVideoContainerSyncingTaskStatement(
          containerId,
          containerData.version,
          now,
          now,
        ),
        ...videoIdsToDelete.map((videoIdToDelete) =>
          deleteVideoTrackStatement(containerId, videoIdToDelete),
        ),
        ...r2VideoDirsToDelete.map((r2VideoDirToDelete) =>
          insertR2KeyCleanupTaskStatement(
            `${r2ContainerDir}/${r2VideoDirToDelete}`,
            now,
            now,
          ),
        ),
        updateVideoTrackStatement(
          {
            done: {
              r2TrackDirname: videoDirAndSize.dirname,
              durationSec: videoDurationSec,
              resolution: videoResolution,
              totalBytes: videoDirAndSize.totalBytes,
            },
          },
          containerId,
          videoId,
        ),
        deleteVideoFormattingTaskStatement(containerId, videoId),
        insertGcsFileCleanupTaskStatement(gcsFilename, now, now),
        deleteR2KeyCleanupTaskStatement(
          `${r2ContainerDir}/${videoDirAndSize.dirname}`,
        ),
        ...audioDirsAndSizes.map((audioDirAndSize, index) =>
          insertAudioTrackStatement(containerId, audioDirAndSize.dirname, {
            name: `${audioTrackRows.length + index + 1}`,
            isDefault: audioTrackRows.length === 0 && index === 0, // If there is no audio tracks and this is the first audio track
            done: {
              r2TrackDirname: audioDirAndSize.dirname,
              totalBytes: audioDirAndSize.totalBytes,
            },
          }),
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
}
