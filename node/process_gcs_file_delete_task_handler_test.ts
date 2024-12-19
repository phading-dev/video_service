import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import {
  GCS_VIDEO_LOCAL_DIR,
  GCS_VIDEO_REMOTE_BUCKET,
} from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  CHECK_GCS_FILE_ROW,
  LIST_GCS_FILE_DELETE_TASKS_ROW,
  checkGcsFile,
  deleteGcsFileDeleteTaskStatement,
  deleteGcsFileStatement,
  insertGcsFileDeleteTaskStatement,
  insertGcsFileStatement,
  listGcsFileDeleteTasks,
} from "../db/sql";
import { ProcessGcsFileDeleteTaskHandler } from "./process_gcs_file_delete_task_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";
import { copyFile, readdir, rm } from "fs/promises";

let VIDEO_FILE_SIZE = 18328570;

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteGcsFileStatement("test_video"),
      deleteGcsFileDeleteTaskStatement("test_video"),
    ]);
    await transaction.commit();
  });
  try {
    await rm(`${GCS_VIDEO_LOCAL_DIR}/test_video`, { force: true });
  } catch (e) {}
}

TEST_RUNNER.run({
  name: "ProcessGcsFileDeleteTaskHandlerTest",
  cases: [
    {
      name: "DeleteFileOnly",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/video_only.mp4",
          `${GCS_VIDEO_LOCAL_DIR}/test_video`,
        );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeleteTaskStatement("test_video", {}, 0, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeleteTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          gcsFilename: "test_video",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(await readdir(GCS_VIDEO_LOCAL_DIR), isArray([]), "ls files");
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([]),
          "checkGcsFile",
        );
        assertThat(
          await listGcsFileDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeleteTasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "CancelUploadOnly",
      execute: async () => {
        // Prepare
        let uploadSessionUrl =
          await CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
            GCS_VIDEO_REMOTE_BUCKET,
            "test_video",
            "video/mp4",
            VIDEO_FILE_SIZE,
          );
        // Wait for GCS to catch up.
        await new Promise<void>((resolve) => setTimeout(resolve, 1000));
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeleteTaskStatement(
              "test_video",
              {
                uploadSessionUrl,
              },
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeleteTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          gcsFilename: "test_video",
          uploadSessionUrl,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        let { urlValid } =
          await CLOUD_STORAGE_CLIENT.checkResumableUploadProgress(
            uploadSessionUrl,
            VIDEO_FILE_SIZE,
          );
        assertThat(urlValid, eq(false), "checkResumableUploadProgress");
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([]),
          "checkGcsFile",
        );
        assertThat(
          await listGcsFileDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeleteTasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "CancelAndDeleteAfterUploaded",
      execute: async () => {
        // Prepare
        let uploadSessionUrl =
          await CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
            GCS_VIDEO_REMOTE_BUCKET,
            "test_video",
            "video/mp4",
            VIDEO_FILE_SIZE,
          );
        await axios.put(
          uploadSessionUrl,
          createReadStream("test_data/video_only.mp4"),
          {
            headers: {
              "Content-Length": VIDEO_FILE_SIZE,
            },
          },
        );
        // Wait for GCS to catch up.
        await new Promise<void>((resolve) => setTimeout(resolve, 1000));
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeleteTaskStatement(
              "test_video",
              {
                uploadSessionUrl,
              },
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeleteTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          gcsFilename: "test_video",
          uploadSessionUrl,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        let { urlValid } =
          await CLOUD_STORAGE_CLIENT.checkResumableUploadProgress(
            uploadSessionUrl,
            VIDEO_FILE_SIZE,
          );
        assertThat(urlValid, eq(true), "checkResumableUploadProgress");
        assertThat(await readdir(GCS_VIDEO_LOCAL_DIR), isArray([]), "ls files");
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([]),
          "checkGcsFile",
        );
        assertThat(
          await listGcsFileDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeleteTasks",
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
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeleteTaskStatement("test_video", {}, 0, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeleteTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );
        handler.interfereFn = () => {
          throw new Error("Fake error");
        };

        // Execute
        handler.handle("", {
          gcsFilename: "test_video",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([
            eqMessage(
              {
                gcsFileFilename: "test_video",
              },
              CHECK_GCS_FILE_ROW,
            ),
          ]),
          "checkGcsFile",
        );
        assertThat(
          await listGcsFileDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "test_video",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 301000,
              },
              LIST_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "listGcsFileDeleteTasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
