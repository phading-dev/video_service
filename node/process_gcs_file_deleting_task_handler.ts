import {
  CLOUD_STORAGE_CLIENT,
  CloudStorageClient,
} from "../common/cloud_storage_client";
import { GCS_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteGcsFileDeletingTaskStatement,
  deleteGcsFileStatement,
  updateGcsFileDeletingTaskStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ProcessGcsFileDeletingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessGcsFileDeletingTaskRequestBody,
  ProcessGcsFileDeletingTaskResponse,
} from "@phading/video_service_interface/node/interface";

export class ProcessGcsFileDeletingTaskHandler extends ProcessGcsFileDeletingTaskHandlerInterface {
  public static create(): ProcessGcsFileDeletingTaskHandler {
    return new ProcessGcsFileDeletingTaskHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      () => Date.now(),
    );
  }

  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public doneCallback: () => void = () => {};
  public interfereFn: () => void = () => {};

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
    await this.claimTask(loggingPrefix, body.gcsFilename);
    this.startProcessingAndCatchError(
      loggingPrefix,
      body.gcsFilename,
      body.uploadSessionUrl,
    );
    return {};
  }

  private async claimTask(
    loggingPrefix: string,
    gcsFilename: string,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessGcsFileDeletingTaskHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateGcsFileDeletingTaskStatement(gcsFilename, delayedTime),
      ]);
      await transaction.commit();
    });
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    gcsFilename: string,
    uploadSessionUrl?: string,
  ): Promise<void> {
    try {
      this.interfereFn();
      console.log(`${loggingPrefix} Deleting GCS file.`);
      await this.gcsClient.deleteFileAndCancelUpload(
        GCS_VIDEO_REMOTE_BUCKET,
        gcsFilename,
        uploadSessionUrl,
      );

      await this.database.runTransactionAsync(async (transaction) => {
        console.log(`${loggingPrefix} Completing the task.`);
        await transaction.batchUpdate([
          deleteGcsFileStatement(gcsFilename),
          deleteGcsFileDeletingTaskStatement(gcsFilename),
        ]);
        await transaction.commit();
      });
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }
}
