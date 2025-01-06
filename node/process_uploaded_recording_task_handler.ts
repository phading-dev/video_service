import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteUploadedRecordingTaskStatement,
  updateUploadedRecordingTaskStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { recordUploaded } from "@phading/product_meter_service_interface/show/node/publisher/client";
import { ProcessUploadedRecordingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessUploadedRecordingTaskRequestBody,
  ProcessUploadedRecordingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessUploadedRecordingTaskHandler extends ProcessUploadedRecordingTaskHandlerInterface {
  public static create(): ProcessUploadedRecordingTaskHandler {
    return new ProcessUploadedRecordingTaskHandler(
      SPANNER_DATABASE,
      SERVICE_CLIENT,
      () => Date.now(),
    );
  }

  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;
  public doneCallback: () => void = () => {};
  public interfereFn: () => void = () => {};

  public constructor(
    private database: Database,
    private serviceClient: NodeServiceClient,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessUploadedRecordingTaskRequestBody,
  ): Promise<ProcessUploadedRecordingTaskResponse> {
    loggingPrefix = `${loggingPrefix} Uploaded recording task for GCS file ${body.gcsFilename}:`;
    await this.claimTask(loggingPrefix, body.gcsFilename);
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.gcsFilename,
      body.accountId,
      body.totalBytes,
    );
    return {};
  }

  private async claimTask(
    loggingPrefix: string,
    gcsFilename: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessUploadedRecordingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateUploadedRecordingTaskStatement(gcsFilename, delayedTime),
      ]);
      await transaction.commit();
    });
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    gcsFilename: string,
    accountId: string,
    totalBytes: number,
  ): Promise<void> {
    try {
      this.interfereFn();
      await recordUploaded(this.serviceClient, {
        name: gcsFilename,
        accountId,
        uploadedBytes: totalBytes,
      });
      await this.database.runTransactionAsync(async (transaction) => {
        await transaction.batchUpdate([
          deleteUploadedRecordingTaskStatement(gcsFilename),
        ]);
        await transaction.commit();
      });
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }
}
