import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { STORAGE_RESUMABLE_UPLOAD_CLIENT } from "../common/storage_resumable_upload_client";
import {
  GET_GCS_KEY_DELETING_TASK_ROW,
  GET_GCS_UPLOAD_FILE_DELETING_TASK_METADATA_ROW,
  deleteGcsKeyDeletingTaskStatement,
  deleteGcsUploadFileDeletingTaskStatement,
  getGcsKeyDeletingTask,
  getGcsUploadFileDeletingTaskMetadata,
  insertGcsUploadFileDeletingTaskStatement,
  listPendingGcsUploadFileDeletingTasks,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessGcsUploadFileDeletingTaskHandler } from "./process_gcs_upload_file_deleting_task_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

let VIDEO_FILE_SIZE = 18328570;

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteGcsUploadFileDeletingTaskStatement({
        gcsUploadFileDeletingTaskFilenameEq: "test_video",
      }),
      deleteGcsKeyDeletingTaskStatement({
        gcsKeyDeletingTaskKeyEq: "test_video",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "ProcessGcsUploadFileDeletingTaskHandlerTest",
  cases: [
    {
      name: "CancelUpload",
      execute: async () => {
        // Prepare
        let uploadSessionUrl =
          await STORAGE_RESUMABLE_UPLOAD_CLIENT.createResumableUploadUrl(
            ENV_VARS.gcsVideoBucketName,
            "test_video",
            VIDEO_FILE_SIZE,
          );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsUploadFileDeletingTaskStatement({
              filename: "test_video",
              uploadSessionUrl,
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsUploadFileDeletingTaskHandler(
          SPANNER_DATABASE,
          STORAGE_RESUMABLE_UPLOAD_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          gcsFilename: "test_video",
          uploadSessionUrl,
        });

        // Verify
        let { urlValid } =
          await STORAGE_RESUMABLE_UPLOAD_CLIENT.checkResumableUploadProgress(
            uploadSessionUrl,
            VIDEO_FILE_SIZE,
          );
        assertThat(urlValid, eq(false), "checkResumableUploadProgress");
        assertThat(
          await listPendingGcsUploadFileDeletingTasks(SPANNER_DATABASE, {
            gcsUploadFileDeletingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "listGcsFileDeletingTasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "test_video",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "GcsKeyDeletingTask",
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
            insertGcsUploadFileDeletingTaskStatement({
              filename: "test_video",
              uploadSessionUrl: "some_url",
              retryCount: 0,
              executionTimeMs: 1000,
              createdTimeMs: 1000,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsUploadFileDeletingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", {
          gcsFilename: "test_video",
        });

        // Verify
        assertThat(
          await getGcsUploadFileDeletingTaskMetadata(SPANNER_DATABASE, {
            gcsUploadFileDeletingTaskFilenameEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsUploadFileDeletingTaskRetryCount: 1,
                gcsUploadFileDeletingTaskExecutionTimeMs: 301000,
              },
              GET_GCS_UPLOAD_FILE_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "GcsUploadFileDeletingTaskMetadata",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
