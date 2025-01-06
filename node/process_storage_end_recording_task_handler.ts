import { SERVICE_CLIENT } from "../common/service_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteStorageEndRecordingTaskStatement,
  updateStorageEndRecordingTaskStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { recordStorageEnd } from "@phading/product_meter_service_interface/show/node/publisher/client";
import { ProcessStorageEndRecordingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessStorageEndRecordingTaskRequestBody,
  ProcessStorageEndRecordingTaskResponse,
} from "@phading/video_service_interface/node/interface";
import { NodeServiceClient } from "@selfage/node_service_client";

export class ProcessStorageEndRecordingTaskHandler extends ProcessStorageEndRecordingTaskHandlerInterface {
  public static create(): ProcessStorageEndRecordingTaskHandler {
    return new ProcessStorageEndRecordingTaskHandler(
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
    body: ProcessStorageEndRecordingTaskRequestBody,
  ): Promise<ProcessStorageEndRecordingTaskResponse> {
    loggingPrefix = `${loggingPrefix}  Storage end recording task for R2 dir ${body.r2Dirname}:`;
    await this.claimTask(loggingPrefix, body.r2Dirname);
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.r2Dirname,
      body.accountId,
      body.endTimeMs,
    );
    return {};
  }

  private async claimTask(
    loggingPrefix: string,
    r2Dirname: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessStorageEndRecordingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateStorageEndRecordingTaskStatement(r2Dirname, delayedTime),
      ]);
      await transaction.commit();
    });
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    r2Dirname: string,
    accountId: string,
    endTimeMs: number,
  ): Promise<void> {
    try {
      this.interfereFn();
      await recordStorageEnd(this.serviceClient, {
        name: r2Dirname,
        accountId,
        storageEndMs: endTimeMs,
      });
      await this.database.runTransactionAsync(async (transaction) => {
        await transaction.batchUpdate([
          deleteStorageEndRecordingTaskStatement(r2Dirname),
        ]);
        await transaction.commit();
      });
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }
}
