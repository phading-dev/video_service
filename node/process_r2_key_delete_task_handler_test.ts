import { R2_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  LIST_R2_KEY_DELETE_TASKS_ROW,
  checkR2Key,
  deleteR2KeyDeleteTaskStatement,
  deleteR2KeyStatement,
  insertR2KeyDeleteTaskStatement,
  insertR2KeyStatement,
  listR2KeyDeleteTasks,
} from "../db/sql";
import { ProcessR2KeyDeleteHandler } from "./process_r2_key_delete_task_handler";
import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

async function uploadFile() {
  await S3_CLIENT.send(
    new PutObjectCommand({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Key: "dir/sub_invalid.txt",
      Body: createReadStream("test_data/sub_invalid.txt"),
    }),
  );
}

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteR2KeyStatement("dir"),
      deleteR2KeyDeleteTaskStatement("dir"),
    ]);
    await transaction.commit();
  });
  await S3_CLIENT.send(
    new DeleteObjectCommand({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Key: "dir/sub_invalid.txt",
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessR2KeyDeleteTaskHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await uploadFile();
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyStatement("dir"),
            insertR2KeyDeleteTaskStatement("dir", 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.handle("", { key: "dir" });

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(0),
          "R2 key deleted",
        );
        assertThat(
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "R2 key delete task",
        );
        assertThat(
          (
            await S3_CLIENT.send(
              new ListObjectsV2Command({
                Bucket: "video-test",
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
            insertR2KeyDeleteTaskStatement("dir", 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.handle("", { key: "dir" });

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(0),
          "R2 key deleted",
        );
        assertThat(
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
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
            insertR2KeyDeleteTaskStatement("dir", 100, 100),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessR2KeyDeleteHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          () => 1000,
        );
        handler.interfereFn = () => {
          throw new Error("Fake error");
        };

        // Execute
        await assertReject(handler.handle("", { key: "dir" }));

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "dir")).length,
          eq(1),
          "R2 key not deleted",
        );
        assertThat(
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "dir",
                r2KeyDeleteTaskExecutionTimestamp: 301000,
              },
              LIST_R2_KEY_DELETE_TASKS_ROW,
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
