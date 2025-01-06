import crypto = require("crypto");
import path = require("path");
import { LOCAL_PLAYLIST_NAME } from "../common/constants";
import {
  GCS_VIDEO_LOCAL_DIR,
  R2_VIDEO_REMOTE_BUCKET,
  SUBTITLE_TEMP_DIR,
} from "../common/env_vars";
import { FILE_UPLOADER, FileUploader } from "../common/r2_file_uploader";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import { VideoContainer } from "../db/schema";
import {
  deleteR2KeyDeletingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  insertStorageStartRecordingTaskStatement,
  updateR2KeyDeletingTaskStatement,
  updateSubtitleFormattingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database, Transaction } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { ProcessSubtitleFormattingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessSubtitleFormattingTaskRequestBody,
  ProcessSubtitleFormattingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/processing_failure_reason";
import { newConflictError } from "@selfage/http_error";
import { createReadStream } from "fs";
import { mkdir, readdir, rm, stat } from "fs/promises";

export interface DirAndSize {
  localFilename?: string;
  bucketDirname?: string;
  totalBytes?: number;
}

export class ProcessSubtitleFormattingTaskHandler extends ProcessSubtitleFormattingTaskHandlerInterface {
  public static create(): ProcessSubtitleFormattingTaskHandler {
    return new ProcessSubtitleFormattingTaskHandler(
      SPANNER_DATABASE,
      FILE_UPLOADER,
      () => Date.now(),
      () => crypto.randomUUID(),
    );
  }

  private static DELAY_CLEANUP_MS = 365 * 24 * 60 * 60 * 1000; // 1 year
  private static DELAY_CLEANUP_ON_ERROR_MS = 5 * 60 * 1000;
  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public doneCallback: () => void = () => {};
  public interfereFormat: () => Promise<void> = () => Promise.resolve();

  public constructor(
    private database: Database,
    private fileUploader: FileUploader,
    private getNow: () => number,
    private generateUuid: () => string,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessSubtitleFormattingTaskRequestBody,
  ): Promise<ProcessSubtitleFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Subtitle formatting task for video container ${body.containerId} GCS filename ${body.gcsFilename}:`;
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
  ): Promise<{ r2RootDirname: string }> {
    let r2RootDirname: string;
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      r2RootDirname = videoContainer.r2RootDirname;
      let delayedTime =
        this.getNow() + ProcessSubtitleFormattingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateSubtitleFormattingTaskStatement(
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
    r2RootDirname: string,
  ): Promise<void> {
    let startTimeMs = this.getNow();
    console.log(`${loggingPrefix} Task starting.`);
    try {
      await this.startProcessingAndCleanupTempDir(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDirname,
      );
      console.log(
        `${loggingPrefix} Task completed! Duration: ${this.getNow() - startTimeMs}ms.`,
      );
    } catch (e) {
      console.error(
        `${loggingPrefix} Task failed! Duration: ${this.getNow() - startTimeMs}ms. ${e.stack ?? e}`,
      );
    }
    // TODO: Add alert when duration is too long.
    this.doneCallback();
  }

  private async startProcessingAndCleanupTempDir(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDirname: string,
  ): Promise<void> {
    let tempDir = `${SUBTITLE_TEMP_DIR}/${gcsFilename}/${this.generateUuid()}`;
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
    r2RootDirname: string,
    tempDir: string,
  ): Promise<void> {
    let { failures, subtitleFiles } = await this.unzipAndValidate(
      loggingPrefix,
      gcsFilename,
      tempDir,
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

    let subtitleDirsAndSizes: Array<DirAndSize> = subtitleFiles.map((file) => {
      return {
        localFilename: file,
        bucketDirname: this.generateUuid(),
      };
    });
    await this.claimR2KeysAndPrepareCleanup(
      loggingPrefix,
      r2RootDirname,
      subtitleDirsAndSizes,
    );
    try {
      await this.toHlsFormat(
        loggingPrefix,
        r2RootDirname,
        tempDir,
        subtitleDirsAndSizes,
      );
      await this.finalize(
        loggingPrefix,
        containerId,
        gcsFilename,
        r2RootDirname,
        subtitleDirsAndSizes,
      );
    } catch (e) {
      await this.cleanupR2Keys(
        loggingPrefix,
        r2RootDirname,
        subtitleDirsAndSizes,
      );
      throw e;
    }
  }

  private async unzipAndValidate(
    loggingPrefix: string,
    gcsFilename: string,
    tempDir: string,
  ): Promise<{
    failures: Array<ProcessingFailureReason>;
    subtitleFiles: Array<string>;
  }> {
    await mkdir(tempDir, {
      recursive: true,
    });
    try {
      await spawnAsync(`${loggingPrefix} When unzip GCS file:`, "unzip", [
        `${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`,
        "-d",
        tempDir,
      ]);
    } catch (e) {
      return {
        failures: [ProcessingFailureReason.SUBTITLE_ZIP_FORMAT_INVALID],
        subtitleFiles: [],
      };
    }

    let subtitleFiles = await this.findFilesRecursively(tempDir, ".");
    subtitleFiles.sort();
    return {
      failures: [],
      subtitleFiles,
    };
  }

  private async findFilesRecursively(
    dir: string,
    subdir: string,
  ): Promise<Array<string>> {
    let items = await readdir(path.join(dir, subdir), { withFileTypes: true });
    let files: string[] = [];
    for (let item of items) {
      let subPath = path.join(subdir, item.name);
      if (item.isDirectory()) {
        let filesFromSubDirectory = await this.findFilesRecursively(
          dir,
          subPath,
        );
        files.push(...filesFromSubDirectory);
      } else {
        files.push(subPath);
      }
    }
    return files;
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
        deleteSubtitleFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileDeletingTaskStatement(gcsFilename, "", now, now),
      ]);
      await transaction.commit();
    });
  }

  private async claimR2KeysAndPrepareCleanup(
    loggingPrefix: string,
    r2RootDirname: string,
    subtitleDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      let delayedTime =
        now + ProcessSubtitleFormattingTaskHandler.DELAY_CLEANUP_MS;
      console.log(
        `${loggingPrefix} Claiming subtitle dirs [${subtitleDirsAndSizes.map((value) => value.bucketDirname).join()}] and set to clean up at ${delayedTime}.`,
      );
      let statements = new Array<Statement>();
      for (let { bucketDirname } of subtitleDirsAndSizes) {
        statements.push(
          insertR2KeyStatement(`${r2RootDirname}/${bucketDirname}`),
          insertR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${bucketDirname}`,
            delayedTime,
            now,
          ),
        );
      }
      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
  }

  private async toHlsFormat(
    loggingPrefix: string,
    r2RootDirname: string,
    tempDir: string,
    subtitleDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Start HLS formatting.`);
    await this.interfereFormat();
    for (let subtitleDirAndSize of subtitleDirsAndSizes) {
      let info = await stat(`${tempDir}/${subtitleDirAndSize.localFilename}`);
      let totalBytes = info.size;
      await this.fileUploader.upload(
        R2_VIDEO_REMOTE_BUCKET,
        `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}/subtitle.vtt`,
        createReadStream(`${tempDir}/${subtitleDirAndSize.localFilename}`),
      );
      totalBytes += 116; // 116 bytes for the file below.
      await this.fileUploader.upload(
        R2_VIDEO_REMOTE_BUCKET,
        `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}/${LOCAL_PLAYLIST_NAME}`,
        `#EXTM3U
#EXT-X-TARGETDURATION:10
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.0,
subtitle.vtt
#EXT-X-ENDLIST
`,
      );
      subtitleDirAndSize.totalBytes = totalBytes;
    }
  }

  private async finalize(
    loggingPrefix: string,
    containerId: string,
    gcsFilename: string,
    r2RootDirname: string,
    subtitleDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(`${loggingPrefix} Task is being finalized.`);
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainer = await this.getValidVideoContainerData(
        transaction,
        containerId,
        gcsFilename,
      );
      videoContainer.processing = undefined; // Processing completed.
      let existingSubtitles = videoContainer.subtitleTracks.length;
      subtitleDirsAndSizes.forEach((subtitleDirAndSize, index) => {
        let name = subtitleDirAndSize.localFilename.split(".")[0];
        videoContainer.subtitleTracks.push({
          r2TrackDirname: subtitleDirAndSize.bucketDirname,
          staging: {
            toAdd: {
              name,
              isDefault: existingSubtitles + index === 0, // If this is the first audio track
              totalBytes: subtitleDirAndSize.totalBytes,
            },
          },
        });
      });
      let now = this.getNow();
      // TODO: Add a task to send notification to users when completed.
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer),
        deleteSubtitleFormattingTaskStatement(containerId, gcsFilename),
        insertGcsFileDeletingTaskStatement(gcsFilename, "", now, now),
        ...subtitleDirsAndSizes.map((subtitleDirAndSize) =>
          insertStorageStartRecordingTaskStatement(
            `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}`,
            {
              r2Dirname: `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}`,
              accountId: videoContainer.accountId,
              totalBytes: subtitleDirAndSize.totalBytes,
              startTimeMs: now,
            },
            now,
            now,
          ),
        ),
        ...subtitleDirsAndSizes.map((subtitleDirAndSize) =>
          deleteR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}`,
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
    if (!videoContainer.processing?.subtitle?.formatting) {
      throw newConflictError(
        `Video container ${containerId} is not in subtitle formatting state.`,
      );
    }
    if (
      videoContainer.processing.subtitle.formatting.gcsFilename !== gcsFilename
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
    subtitleDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up subtitle dirs [${subtitleDirsAndSizes.map((value) => value.bucketDirname).join()}] in ${ProcessSubtitleFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() +
        ProcessSubtitleFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS;
      await transaction.batchUpdate(
        subtitleDirsAndSizes.map((value) =>
          updateR2KeyDeletingTaskStatement(
            `${r2RootDirname}/${value.bucketDirname}`,
            delayedTime,
          ),
        ),
      );
      await transaction.commit();
    });
  }
}
