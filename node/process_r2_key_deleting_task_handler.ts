import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  getR2KeyDeletingTaskMetadata,
  updateR2KeyDeletingTaskMetadataStatement,
} from "../db/sql";
import { ENV_VARS } from "../env";
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
import { newBadRequestError } from "@selfage/http_error";
import { Ref } from "@selfage/ref";
import { ProcessTaskHandlerWrapper } from "@selfage/service_handler/process_task_handler_wrapper";

export class ProcessR2KeyDeleteHandler extends ProcessR2KeyDeletingTaskHandlerInterface {
  public static create(): ProcessR2KeyDeleteHandler {
    return new ProcessR2KeyDeleteHandler(SPANNER_DATABASE, S3_CLIENT, () =>
      Date.now(),
    );
  }

  private taskHandler: ProcessTaskHandlerWrapper;

  public constructor(
    private database: Database,
    private s3Client: Ref<S3Client>,
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
    body: ProcessR2KeyDeletingTaskRequestBody,
  ): Promise<ProcessR2KeyDeletingTaskResponse> {
    loggingPrefix = `${loggingPrefix} R2 key cleanup task for ${body.key}:`;
    await this.taskHandler.wrap(
      loggingPrefix,
      () => this.claimTask(loggingPrefix, body),
      () => this.processTask(loggingPrefix, body),
    );
    return {};
  }

  public async claimTask(
    loggingPrefix: string,
    body: ProcessR2KeyDeletingTaskRequestBody,
  ): Promise<void> {
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getR2KeyDeletingTaskMetadata(transaction, body.key);
      if (rows.length === 0) {
        throw newBadRequestError("Task is not found.");
      }
      let task = rows[0];
      await transaction.batchUpdate([
        updateR2KeyDeletingTaskMetadataStatement(
          body.key,
          task.r2KeyDeletingTaskRetryCount + 1,
          this.getNow() +
            this.taskHandler.getBackoffTime(task.r2KeyDeletingTaskRetryCount),
        ),
      ]);
      await transaction.commit();
    });
  }

  public async processTask(
    loggingPrefix: string,
    body: ProcessR2KeyDeletingTaskRequestBody,
  ): Promise<void> {
    while (true) {
      let response = await this.s3Client.val.send(
        new ListObjectsV2Command({
          Bucket: ENV_VARS.r2VideoBucketName,
          Prefix: body.key,
          MaxKeys: 1000,
        }),
      );
      if (response.Contents) {
        await this.s3Client.val.send(
          new DeleteObjectsCommand({
            Bucket: ENV_VARS.r2VideoBucketName,
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
      await transaction.batchUpdate([
        deleteR2KeyStatement(body.key),
        deleteR2KeyDeletingTaskStatement(body.key),
      ]);
      await transaction.commit();
    });
  }
}
