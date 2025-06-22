import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteStorageStartRecordingTaskStatement,
  getStorageStartRecordingTaskMetadata,
  updateStorageStartRecordingTaskMetadataStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { newRecordStorageStartRequest } from "@phading/meter_service_interface/show/node/publisher/client";
import { ProcessStorageStartRecordingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessStorageStartRecordingTaskRequestBody,
  ProcessStorageStartRecordingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessStorageStartRecordingTaskHandler extends ProcessStorageStartRecordingTaskHandlerInterface {
  public static create(): ProcessStorageStartRecordingTaskHandler {
    return new ProcessStorageStartRecordingTaskHandler(
      SPANNER_DATABASE,
      SERVICE_CLIENT,
      () => Date.now(),
    );
  }

  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private serviceClient: NodeServiceClient,
    private getNow: () => number,
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
    body: ProcessStorageStartRecordingTaskRequestBody,
  ): Promise<ProcessStorageStartRecordingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Storage start recording task for R2 dir ${body.r2Dirname}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessStorageStartRecordingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getStorageStartRecordingTaskMetadata(transaction, {
        storageStartRecordingTaskR2DirnameEq: body.r2Dirname,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateStorageStartRecordingTaskMetadataStatement({
          storageStartRecordingTaskR2DirnameEq: body.r2Dirname,
          setRetryCount: task.storageStartRecordingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.storageStartRecordingTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessStorageStartRecordingTaskRequestBody,
  ): Promise<void> {
    console.log(`${loggingPrefix} Start recording...`);
    await this.serviceClient.send(
      newRecordStorageStartRequest({
        name: body.r2Dirname,
        accountId: body.accountId,
        storageBytes: body.totalBytes,
        storageStartMs: body.startTimeMs,
      }),
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        deleteStorageStartRecordingTaskStatement({
          storageStartRecordingTaskR2DirnameEq: body.r2Dirname,
        }),
      ]);
      await transaction.commit();
    });
  }
}
