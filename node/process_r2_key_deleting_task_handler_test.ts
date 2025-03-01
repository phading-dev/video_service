import "../local/env";
import { S3_CLIENT, initS3Client } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_R2_KEY_DELETING_TASK_METADATA_ROW,
  checkR2Key,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  getR2KeyDeletingTaskMetadata,
  insertR2KeyDeletingTaskStatement,
  insertR2KeyStatement,
  listPendingR2KeyDeletingTasks,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessR2KeyDeleteHandler } from "./process_r2_key_deleting_task_handler";
import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { eqMessage } from "@selfage/message/test_matcher";
import { Ref } from "@selfage/ref";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

async function uploadFile() {
  await S3_CLIENT.val.send(
    new PutObjectCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Key: "dir/sub_invalid.txt",
      Body: createReadStream("test_data/sub_invalid.txt"),
    }),
  );
}

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteR2KeyStatement("dir"),
      deleteR2KeyDeletingTaskStatement("dir"),
    ]);
    await transaction.commit();
  });
  await S3_CLIENT.val.send(
    new DeleteObjectCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Key: "dir/sub_invalid.txt",
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessR2KeyDeletingTaskHandlerTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await uploadFile();
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyStatement("dir"),
            insertR2KeyDeletingTaskStatement("dir", 0, 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", { key: "dir" });

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(0),
          "R2 key deleted",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "R2 key delete task",
        );
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "dir",
              }),
            )
          ).KeyCount,
          eq(0),
          "File deleted",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "AlreadyDeleted",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyStatement("dir"),
            insertR2KeyDeletingTaskStatement("dir", 0, 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", { key: "dir" });

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(0),
          "R2 key deleted",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "R2 key delete task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DeleteFailed",
      execute: async () => {
        // Prepare
        await uploadFile();
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyStatement("dir"),
            insertR2KeyDeletingTaskStatement("dir", 0, 100, 100),
          ]);
          await transaction.commit();
        });
        let s3ClientRef = new Ref<S3Client>();
        let s3ClientMock = new (class extends S3Client {
          public async send(command: any) {
            throw new Error("Fake error");
          }
        })();
        s3ClientRef.val = s3ClientMock;
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          s3ClientRef,
          () => 1000,
        );

        // Execute
        let error = await assertReject(handler.processTask("", { key: "dir" }));

        // Verify
        assertThat(error, eqError(new Error("Fake error")), "Error");
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(1),
          "R2 key not deleted",
        );
        assertThat(
          await getR2KeyDeletingTaskMetadata(SPANNER_DATABASE, "dir"),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 100,
              },
              GET_R2_KEY_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "R2 key delete task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ClaimTask",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyDeletingTaskStatement("dir", 0, 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", { key: "dir" });

        // Verify
        assertThat(
          await getR2KeyDeletingTaskMetadata(SPANNER_DATABASE, "dir"),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskRetryCount: 1,
                r2KeyDeletingTaskExecutionTimeMs: 301000,
              },
              GET_R2_KEY_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "R2 key delete task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
