import { SPANNER_DATABASE } from "../common/spanner_database";
import { STORAGE_CLIENT } from "../common/storage_client";
import {
  deleteGcsKeyDeletingTaskStatement,
  deleteGcsKeyStatement,
  getGcsKeyDeletingTaskMetadata,
  updateGcsKeyDeletingTaskMetadataStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { Database } from "@google-cloud/spanner";
import { Storage } from "@google-cloud/storage";
import { ProcessGcsKeyDeletingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessGcsKeyDeletingTaskRequestBody,
  ProcessGcsKeyDeletingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessGcsKeyDeletingTaskHandler extends ProcessGcsKeyDeletingTaskHandlerInterface {
  public static create(): ProcessGcsKeyDeletingTaskHandler {
    return new ProcessGcsKeyDeletingTaskHandler(
      SPANNER_DATABASE,
      STORAGE_CLIENT,
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
    private storageClient: Storage,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessGcsKeyDeletingTaskRequestBody,
  ): Promise<ProcessGcsKeyDeletingTaskResponse> {
    loggingPrefix = `${loggingPrefix} GCS key deleting task for ${body.key}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessGcsKeyDeletingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getGcsKeyDeletingTaskMetadata(transaction, {
        gcsKeyDeletingTaskKeyEq: body.key,
      });
      if (rows.length === 0) {
        throw newBadRequestError(`Task is not found.`);
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateGcsKeyDeletingTaskMetadataStatement({
          gcsKeyDeletingTaskKeyEq: body.key,
          setRetryCount: task.gcsKeyDeletingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(task.gcsKeyDeletingTaskRetryCount),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessGcsKeyDeletingTaskRequestBody,
  ): Promise<void> {
    await this.storageClient.bucket(ENV_VARS.gcsVideoBucketName).deleteFiles({
      prefix: body.key,
      autoPaginate: true,
    });
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        deleteGcsKeyStatement({ gcsKeyKeyEq: body.key }),
        deleteGcsKeyDeletingTaskStatement({
          gcsKeyDeletingTaskKeyEq: body.key,
        }),
      ]);
      await transaction.commit();
    });
  }
}
