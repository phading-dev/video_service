import { R2_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  updateR2KeyDeletingTaskStatement,
} from "../db/sql";
import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Database } from "@google-cloud/spanner";
import { ProcessR2KeyDeletingTaskHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ProcessR2KeyDeletingTaskRequestBody,
  ProcessR2KeyDeletingTaskResponse,
} from "@phading/video_service_interface/node/interface";

export class ProcessR2KeyDeleteHandler extends ProcessR2KeyDeletingTaskHandlerInterface {
  public static create(): ProcessR2KeyDeleteHandler {
    return new ProcessR2KeyDeleteHandler(SPANNER_DATABASE, S3_CLIENT, () =>
      Date.now(),
    );
  }

  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public doneCallback: () => void = () => {};
  public interfereFn: () => void = () => {};

  public constructor(
    private database: Database,
    private s3Client: S3Client,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ProcessR2KeyDeletingTaskRequestBody,
  ): Promise<ProcessR2KeyDeletingTaskResponse> {
    loggingPrefix = `${loggingPrefix} R2 key cleanup task for ${body.key}:`;
    await this.claimTask(loggingPrefix, body.key);
    this.startProcessingAndCatchError(loggingPrefix, body.key);
    return {};
  }

  private async claimTask(loggingPrefix: string, key: string): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessR2KeyDeleteHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateR2KeyDeletingTaskStatement(key, delayedTime),
      ]);
      await transaction.commit();
    });
  }

  private async startProcessingAndCatchError(
    loggingPrefix: string,
    key: string,
  ): Promise<void> {
    try {
      this.interfereFn();
      console.log(`${loggingPrefix} Deleting R2 key.`);
      while (true) {
        let response = await this.s3Client.send(
          new ListObjectsV2Command({
            Bucket: R2_VIDEO_REMOTE_BUCKET,
            Prefix: key,
            MaxKeys: 1000,
          }),
        );
        if (response.Contents) {
          await this.s3Client.send(
            new DeleteObjectsCommand({
              Bucket: R2_VIDEO_REMOTE_BUCKET,
              Delete: {
                Objects: response.Contents.map((content) => ({
                  Key: content.Key,
                })),
              },
            }),
          );
        }
        if (!response.IsTruncated) {
          break;
        }
      }

      await this.database.runTransactionAsync(async (transaction) => {
        console.log(`${loggingPrefix} Completing the task.`);
        await transaction.batchUpdate([
          deleteR2KeyStatement(key),
          deleteR2KeyDeletingTaskStatement(key),
        ]);
        await transaction.commit();
      });
    } catch (e) {
      console.error(`${loggingPrefix} Task failed! ${e.stack ?? e}`);
    }
    this.doneCallback();
  }
}
