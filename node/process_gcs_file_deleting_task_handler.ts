import {
  CLOUD_STORAGE_CLIENT,
  CloudStorageClient,
} from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteGcsFileDeletingTaskStatement,
  deleteGcsFileStatement,
  getGcsFileDeletingTaskMetadata,
  updateGcsFileDeletingTaskMetadataStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { Database } from "@google-cloud/spanner";
import { ProcessGcsFileDeletingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessGcsFileDeletingTaskRequestBody,
  ProcessGcsFileDeletingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessGcsFileDeletingTaskHandler extends ProcessGcsFileDeletingTaskHandlerInterface {
  public static create(): ProcessGcsFileDeletingTaskHandler {
    return new ProcessGcsFileDeletingTaskHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
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
    private gcsClient: CloudStorageClient,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessGcsFileDeletingTaskRequestBody,
  ): Promise<ProcessGcsFileDeletingTaskResponse> {
    loggingPrefix = `${loggingPrefix} GCS file cleanup task for ${body.gcsFilename}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessGcsFileDeletingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getGcsFileDeletingTaskMetadata(transaction, {
        gcsFileDeletingTaskFilenameEq: body.gcsFilename,
      });
      if (rows.length === 0) {
        throw newBadRequestError(`Task is not found.`);
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateGcsFileDeletingTaskMetadataStatement({
          gcsFileDeletingTaskFilenameEq: body.gcsFilename,
          setRetryCount: task.gcsFileDeletingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(task.gcsFileDeletingTaskRetryCount),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessGcsFileDeletingTaskRequestBody,
  ): Promise<void> {
    await this.gcsClient.deleteFileAndCancelUpload(
      ENV_VARS.gcsVideoBucketName,
      body.gcsFilename,
      body.uploadSessionUrl,
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        deleteGcsFileStatement({ gcsFileFilenameEq: body.gcsFilename }),
        deleteGcsFileDeletingTaskStatement({
          gcsFileDeletingTaskFilenameEq: body.gcsFilename,
        }),
      ]);
      await transaction.commit();
    });
  }
}
