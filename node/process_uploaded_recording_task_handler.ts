import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteUploadedRecordingTaskStatement,
  getUploadedRecordingTaskMetadata,
  updateUploadedRecordingTaskMetadataStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { newRecordUploadedRequest } from "@phading/meter_service_interface/show/node/publisher/client";
import { ProcessUploadedRecordingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessUploadedRecordingTaskRequestBody,
  ProcessUploadedRecordingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";
import { NodeServiceClient } from "@selfage/node_service_client";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessUploadedRecordingTaskHandler extends ProcessUploadedRecordingTaskHandlerInterface {
  public static create(): ProcessUploadedRecordingTaskHandler {
    return new ProcessUploadedRecordingTaskHandler(
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
    body: ProcessUploadedRecordingTaskRequestBody,
  ): Promise<ProcessUploadedRecordingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Uploaded recording task for GCS file ${body.gcsKey}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessUploadedRecordingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getUploadedRecordingTaskMetadata(transaction, {
        uploadedRecordingTaskGcsKeyEq: body.gcsKey,
      });
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateUploadedRecordingTaskMetadataStatement({
          uploadedRecordingTaskGcsKeyEq: body.gcsKey,
          setRetryCount: task.uploadedRecordingTaskRetryCount + 1,
          setExecutionTimeMs:
            this.getNow() +
            this.taskHandler.getBackoffTime(
              task.uploadedRecordingTaskRetryCount,
            ),
        }),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessUploadedRecordingTaskRequestBody,
  ): Promise<void> {
    await this.serviceClient.send(
      newRecordUploadedRequest({
        name: body.gcsKey,
        accountId: body.accountId,
        uploadedBytes: body.totalBytes,
      }),
    );
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        deleteUploadedRecordingTaskStatement({
          uploadedRecordingTaskGcsKeyEq: body.gcsKey,
        }),
      ]);
      await transaction.commit();
    });
  }
}
