import crypto = require("crypto");
import path = require("path");
import {
  LOCAL_PLAYLIST_NAME,
  LOCAL_SUBTITLE_NAME,
  SUBTITLE_TEMP_DIR,
} from "../common/constants";
import { FILE_UPLOADER, FileUploader } from "../common/r2_file_uploader";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { spawnAsync } from "../common/spawn";
import {
  GetVideoContainerRow,
  deleteR2KeyDeletingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  getSubtitleFormattingTaskMetadata,
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  insertStorageStartRecordingTaskStatement,
  updateR2KeyDeletingTaskMetadataStatement,
  updateSubtitleFormattingTaskMetadataStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { Database, Transaction } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { ProcessSubtitleFormattingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessSubtitleFormattingTaskRequestBody,
  ProcessSubtitleFormattingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/last_processing_failure";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";
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
  public interfereFormat: () => Promise<void> = () => Promise.resolve();
  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
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
    body: ProcessSubtitleFormattingTaskRequestBody,
  ): Promise<ProcessSubtitleFormattingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Subtitle formatting task for video container ${body.containerId} GCS filename ${body.gcsFilename}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessSubtitleFormattingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getSubtitleFormattingTaskMetadata(transaction, {
        subtitleFormattingTaskContainerIdEq: body.containerId,
        subtitleFormattingTaskGcsFilenameEq: body.gcsFilename,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      let delayedExecutionTime =
        this.getNow() +
        this.taskHandler.getBackoffTime(task.subtitleFormattingTaskRetryCount);
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedExecutionTime}.`,
      );
      await transaction.batchUpdate([
        updateSubtitleFormattingTaskMetadataStatement({
          subtitleFormattingTaskContainerIdEq: body.containerId,
          subtitleFormattingTaskGcsFilenameEq: body.gcsFilename,
          setRetryCount: task.subtitleFormattingTaskRetryCount + 1,
          setExecutionTimeMs: delayedExecutionTime,
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessSubtitleFormattingTaskRequestBody,
  ): Promise<void> {
    let tempDir = `${SUBTITLE_TEMP_DIR}/${body.gcsFilename}/${this.generateUuid()}`;
    try {
      await this.startProcessing(
        loggingPrefix,
        body.containerId,
        body.gcsFilename,
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
    tempDir: string,
  ): Promise<void> {
    let { videoContainerData } = await this.getValidVideoContainerData(
      this.database,
      containerId,
      gcsFilename,
    );
    let r2RootDirname = videoContainerData.r2RootDirname;

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
        `${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`,
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
        deleteSubtitleFormattingTaskStatement({
          subtitleFormattingTaskContainerIdEq: containerId,
          subtitleFormattingTaskGcsFilenameEq: gcsFilename,
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
          insertR2KeyStatement({ key: `${r2RootDirname}/${bucketDirname}` }),
          insertR2KeyDeletingTaskStatement({
            key: `${r2RootDirname}/${bucketDirname}`,
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
        ENV_VARS.r2VideoBucketName,
        `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}/${LOCAL_SUBTITLE_NAME}`,
        createReadStream(`${tempDir}/${subtitleDirAndSize.localFilename}`),
      );
      totalBytes += 116; // 116 bytes for the file below.
      await this.fileUploader.upload(
        ENV_VARS.r2VideoBucketName,
        `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}/${LOCAL_PLAYLIST_NAME}`,
        `#EXTM3U
#EXT-X-TARGETDURATION:10
#EXT-X-VERSION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:10.0,
${LOCAL_SUBTITLE_NAME}
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
      let { videoContainerAccountId, videoContainerData } =
        await this.getValidVideoContainerData(
          transaction,
          containerId,
          gcsFilename,
        );
      videoContainerData.processing = undefined; // Processing completed.
      subtitleDirsAndSizes.forEach((subtitleDirAndSize) => {
        let name = subtitleDirAndSize.localFilename.split(".")[0];
        videoContainerData.subtitleTracks.push({
          r2TrackDirname: subtitleDirAndSize.bucketDirname,
          totalBytes: subtitleDirAndSize.totalBytes,
          staging: {
            toAdd: {
              name,
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
        deleteSubtitleFormattingTaskStatement({
          subtitleFormattingTaskContainerIdEq: containerId,
          subtitleFormattingTaskGcsFilenameEq: gcsFilename,
        }),
        insertGcsFileDeletingTaskStatement({
          filename: gcsFilename,
          uploadSessionUrl: "",
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        ...subtitleDirsAndSizes.map((subtitleDirAndSize) =>
          insertStorageStartRecordingTaskStatement({
            r2Dirname: `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}`,
            payload: {
              accountId: videoContainerAccountId,
              totalBytes: subtitleDirAndSize.totalBytes,
              startTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...subtitleDirsAndSizes.map((subtitleDirAndSize) =>
          deleteR2KeyDeletingTaskStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${subtitleDirAndSize.bucketDirname}`,
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
    if (!videoContainer.videoContainerData.processing?.subtitle?.formatting) {
      throw newConflictError(
        `Video container ${containerId} is not in subtitle formatting state.`,
      );
    }
    if (
      videoContainer.videoContainerData.processing.subtitle.formatting
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
    subtitleDirsAndSizes: Array<DirAndSize>,
  ): Promise<void> {
    console.log(
      `${loggingPrefix} Encountered error. Cleaning up subtitle dirs [${subtitleDirsAndSizes.map((value) => value.bucketDirname).join()}] in ${ProcessSubtitleFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS} ms.`,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate(
        subtitleDirsAndSizes.map((value) =>
          updateR2KeyDeletingTaskMetadataStatement({
            r2KeyDeletingTaskKeyEq: `${r2RootDirname}/${value.bucketDirname}`,
            setRetryCount: 0,
            setExecutionTimeMs:
              this.getNow() +
              ProcessSubtitleFormattingTaskHandler.DELAY_CLEANUP_ON_ERROR_MS,
          }),
        ),
      );
      await transaction.commit();
    });
  }
}
