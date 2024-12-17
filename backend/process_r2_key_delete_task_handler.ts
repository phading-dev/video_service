import { R2_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteR2KeyDeleteTaskStatement,
  deleteR2KeyStatement,
  updateR2KeyDeleteTaskStatement,
} from "../db/sql";
import {
  ProcessR2KeyDeleteTaskRequestBody,
  ProcessR2KeyDeleteTaskResponse,
} from "../interface";
import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { Database } from "@google-cloud/spanner";

export class ProcessR2KeyDeleteHandler {
  public static create(): ProcessR2KeyDeleteHandler {
    return new ProcessR2KeyDeleteHandler(SPANNER_DATABASE, S3_CLIENT, () =>
      Date.now(),
    );
  }

  private static RETRY_BACKOFF_MS = 5 * 60 * 1000;

  public interfereFn: () => void = () => {};

  public constructor(
    private database: Database,
    private s3Client: S3Client,
    private getNow: () => number,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: ProcessR2KeyDeleteTaskRequestBody,
  ): Promise<ProcessR2KeyDeleteTaskResponse> {
    loggingPrefix = `${loggingPrefix} R2 key cleanup task for ${body.key}:`;
    await this.database.runTransactionAsync(async (transaction) => {
      let delayedTime =
        this.getNow() + ProcessR2KeyDeleteHandler.RETRY_BACKOFF_MS;
      console.log(
        `${loggingPrefix} Claiming the task by delaying it to ${delayedTime}.`,
      );
      await transaction.batchUpdate([
        updateR2KeyDeleteTaskStatement(body.key, delayedTime),
      ]);
      await transaction.commit();
    });

    this.interfereFn();
    console.log(`${loggingPrefix} Deleting R2 key.`);
    while (true) {
      let response = await this.s3Client.send(
        new ListObjectsV2Command({
          Bucket: R2_VIDEO_REMOTE_BUCKET,
          Prefix: body.key,
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
        deleteR2KeyStatement(body.key),
        deleteR2KeyDeleteTaskStatement(body.key),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
