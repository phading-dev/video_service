import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { STORAGE_CLIENT } from "../common/storage_client";
import {
  GET_GCS_KEY_DELETING_TASK_METADATA_ROW,
  deleteGcsKeyDeletingTaskStatement,
  deleteGcsKeyStatement,
  getGcsKey,
  getGcsKeyDeletingTaskMetadata,
  insertGcsKeyDeletingTaskStatement,
  insertGcsKeyStatement,
  listPendingGcsKeyDeletingTasks,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessGcsKeyDeletingTaskHandler } from "./process_gcs_key_deleting_task_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { copyFile, cp, readdir, rm } from "fs/promises";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteGcsKeyStatement({ gcsKeyKeyEq: "test_video" }),
      deleteGcsKeyDeletingTaskStatement({
        gcsKeyDeletingTaskKeyEq: "test_video",
      }),
    ]);
    await transaction.commit();
  });
  try {
    await rm(`${ENV_VARS.gcsVideoMountedLocalDir}/test_video`, {
      recursive: true,
      force: true,
    });
  } catch (e) {}
}

TEST_RUNNER.run({
  name: "ProcessGcsKeyDeletingTaskHandlerTest",
  cases: [
    {
      name: "DeleteFile",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/video_only.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/test_video`,
        );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsKeyStatement({ key: "test_video" }),
            insertGcsKeyDeletingTaskStatement({
              key: "test_video",
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsKeyDeletingTaskHandler(
          SPANNER_DATABASE,
          STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          key: "test_video",
        });

        // Verify
        assertThat(
          await readdir(ENV_VARS.gcsVideoMountedLocalDir),
          isArray([]),
          "ls files",
        );
        assertThat(
          await getGcsKey(SPANNER_DATABASE, {
            gcsKeyKeyEq: "test_video",
          }),
          isArray([]),
          "getGcsKey",
        );
        assertThat(
          await listPendingGcsKeyDeletingTasks(SPANNER_DATABASE, {
            gcsKeyDeletingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "listGcsKeyDeletingTasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DeleteDirectory",
      execute: async () => {
        // Prepare
        await cp(
          "test_data/video_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/test_video`,
          { recursive: true },
        );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsKeyStatement({ key: "test_video" }),
            insertGcsKeyDeletingTaskStatement({
              key: "test_video",
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsKeyDeletingTaskHandler(
          SPANNER_DATABASE,
          STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          key: "test_video",
        });

        // Verify
        assertThat(
          await readdir(ENV_VARS.gcsVideoMountedLocalDir),
          isArray([]),
          "ls files",
        );
        assertThat(
          await getGcsKey(SPANNER_DATABASE, {
            gcsKeyKeyEq: "test_video",
          }),
          isArray([]),
          "getGcsKey",
        );
        assertThat(
          await listPendingGcsKeyDeletingTasks(SPANNER_DATABASE, {
            gcsKeyDeletingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "listGcsKeyDeletingTasks",
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
            insertGcsKeyDeletingTaskStatement({
              key: "test_video",
              retryCount: 0,
              executionTimeMs: 1000,
              createdTimeMs: 1000,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsKeyDeletingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", {
          key: "test_video",
        });

        // Verify
        assertThat(
          await getGcsKeyDeletingTaskMetadata(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskRetryCount: 1,
                gcsKeyDeletingTaskExecutionTimeMs: 301000,
              },
              GET_GCS_KEY_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "GcsKeyDeletingTask",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
