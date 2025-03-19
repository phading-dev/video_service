import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteStorageEndRecordingTaskStatement,
  getStorageEndRecordingTaskMetadata,
  updateStorageEndRecordingTaskMetadataStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { newRecordStorageEndRequest } from "@phading/meter_service_interface/show/node/publisher/client";
import { ProcessStorageEndRecordingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessStorageEndRecordingTaskRequestBody,
  ProcessStorageEndRecordingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessStorageEndRecordingTaskHandler extends ProcessStorageEndRecordingTaskHandlerInterface {
  public static create(): ProcessStorageEndRecordingTaskHandler {
    return new ProcessStorageEndRecordingTaskHandler(
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
    body: ProcessStorageEndRecordingTaskRequestBody,
  ): Promise<ProcessStorageEndRecordingTaskResponse> {
    loggingPrefix = `${loggingPrefix}  Storage end recording task for R2 dir ${body.r2Dirname}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessStorageEndRecordingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getStorageEndRecordingTaskMetadata(transaction, {
        storageEndRecordingTaskR2DirnameEq: body.r2Dirname,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateStorageEndRecordingTaskMetadataStatement({
          storageEndRecordingTaskR2DirnameEq: body.r2Dirname,
          setRetryCount: task.storageEndRecordingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.storageEndRecordingTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessStorageEndRecordingTaskRequestBody,
  ): Promise<void> {
    await this.serviceClient.send(
      newRecordStorageEndRequest({
        name: body.r2Dirname,
        accountId: body.accountId,
        storageEndMs: body.endTimeMs,
      }),
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        deleteStorageEndRecordingTaskStatement({
          storageEndRecordingTaskR2DirnameEq: body.r2Dirname,
        }),
      ]);
      await transaction.commit();
    });
  }
}
