import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  STORAGE_RESUMABLE_UPLOAD_CLIENT,
  StorageResumableUploadClient,
} from "../common/storage_resumable_upload_client";
import {
  deleteGcsUploadFileDeletingTaskStatement,
  getGcsUploadFileDeletingTask,
  insertGcsKeyDeletingTaskStatement,
  updateGcsUploadFileDeletingTaskMetadataStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ProcessGcsUploadFileDeletingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessGcsUploadFileDeletingTaskRequestBody,
  ProcessGcsUploadFileDeletingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessGcsUploadFileDeletingTaskHandler extends ProcessGcsUploadFileDeletingTaskHandlerInterface {
  public static create(): ProcessGcsUploadFileDeletingTaskHandler {
    return new ProcessGcsUploadFileDeletingTaskHandler(
      SPANNER_DATABASE,
      STORAGE_RESUMABLE_UPLOAD_CLIENT,
      () => Date.now(),
    );
  }

  private taskHandler = ProcessTaskHandlerWrapper.create(
    this.descriptor,
    5 * 60 * 1000,
    24 * 60 * 60 * 1000,
  );

  public constructor(
    private database: Database,
    private resumableUploadClient: StorageResumableUploadClient,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessGcsUploadFileDeletingTaskRequestBody,
  ): Promise<ProcessGcsUploadFileDeletingTaskResponse> {
    loggingPrefix = `${loggingPrefix} GCS upload file deleting task for ${body.gcsFilename}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessGcsUploadFileDeletingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getGcsUploadFileDeletingTask(transaction, {
        gcsUploadFileDeletingTaskFilenameEq: body.gcsFilename,
      });
      if (rows.length === 0) {
        throw newBadRequestError(`Task is not found.`);
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateGcsUploadFileDeletingTaskMetadataStatement({
          gcsUploadFileDeletingTaskFilenameEq: body.gcsFilename,
          setRetryCount: task.gcsUploadFileDeletingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.gcsUploadFileDeletingTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessGcsUploadFileDeletingTaskRequestBody,
  ): Promise<void> {
    await this.resumableUploadClient.cancelUpload(body.uploadSessionUrl);
    await this.database.runTransactionAsync(async (transaction) => {
      let now = this.getNow();
      await transaction.batchUpdate([
        deleteGcsUploadFileDeletingTaskStatement({
          gcsUploadFileDeletingTaskFilenameEq: body.gcsFilename,
        }),
        insertGcsKeyDeletingTaskStatement({
          key: body.gcsFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
      ]);
      await transaction.commit();
    });
  }
}
