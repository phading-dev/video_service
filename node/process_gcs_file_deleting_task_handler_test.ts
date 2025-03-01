import "../local/env";
import axios from "axios";
import {
  CLOUD_STORAGE_CLIENT,
  CloudStorageClient,
} from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  CHECK_GCS_FILE_ROW,
  GET_GCS_FILE_DELETING_TASK_METADATA_ROW,
  checkGcsFile,
  deleteGcsFileDeletingTaskStatement,
  deleteGcsFileStatement,
  getGcsFileDeletingTaskMetadata,
  insertGcsFileDeletingTaskStatement,
  insertGcsFileStatement,
  listPendingGcsFileDeletingTasks,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessGcsFileDeletingTaskHandler } from "./process_gcs_file_deleting_task_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";
import { copyFile, readdir, rm } from "fs/promises";

let VIDEO_FILE_SIZE = 18328570;

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteGcsFileStatement("test_video"),
      deleteGcsFileDeletingTaskStatement("test_video"),
    ]);
    await transaction.commit();
  });
  try {
    await rm(`${ENV_VARS.gcsVideoMountedLocalDir}/test_video`, { force: true });
  } catch (e) {}
}

TEST_RUNNER.run({
  name: "ProcessGcsFileDeletingTaskHandlerTest",
  cases: [
    {
      name: "DeleteFileOnly",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/video_only.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/test_video`,
        );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeletingTaskStatement("test_video", "", 0, 0, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeletingTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          gcsFilename: "test_video",
        });

        // Verify
        assertThat(
          await readdir(ENV_VARS.gcsVideoMountedLocalDir),
          isArray([]),
          "ls files",
        );
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([]),
          "checkGcsFile",
        );
        assertThat(
          await listPendingGcsFileDeletingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeletingTasks",
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
            ENV_VARS.gcsVideoBucketName,
            "test_video",
            "video/mp4",
            VIDEO_FILE_SIZE,
          );
        // Wait for GCS to catch up.
        await new Promise<void>((resolve) => setTimeout(resolve, 1000));
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileStatement("test_video"),
            insertGcsFileDeletingTaskStatement(
              "test_video",
              uploadSessionUrl,
              0,
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeletingTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          gcsFilename: "test_video",
          uploadSessionUrl,
        });

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
          await listPendingGcsFileDeletingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeletingTasks",
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
            ENV_VARS.gcsVideoBucketName,
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
            insertGcsFileDeletingTaskStatement(
              "test_video",
              uploadSessionUrl,
              0,
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeletingTaskHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          gcsFilename: "test_video",
          uploadSessionUrl,
        });

        // Verify
        let { urlValid } =
          await CLOUD_STORAGE_CLIENT.checkResumableUploadProgress(
            uploadSessionUrl,
            VIDEO_FILE_SIZE,
          );
        assertThat(urlValid, eq(true), "checkResumableUploadProgress");
        assertThat(
          await readdir(ENV_VARS.gcsVideoMountedLocalDir),
          isArray([]),
          "ls files",
        );
        assertThat(
          await checkGcsFile(SPANNER_DATABASE, "test_video"),
          isArray([]),
          "checkGcsFile",
        );
        assertThat(
          await listPendingGcsFileDeletingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "listGcsFileDeletingTasks",
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
            insertGcsFileDeletingTaskStatement("test_video", "", 0, 0, 0),
          ]);
          await transaction.commit();
        });
        let cloudStorageClientMock = new (class extends CloudStorageClient {
          public constructor() {
            super(undefined, undefined);
          }
          public async deleteFileAndCancelUpload() {
            throw new Error("Fake error");
          }
        })();
        let handler = new ProcessGcsFileDeletingTaskHandler(
          SPANNER_DATABASE,
          cloudStorageClientMock,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            gcsFilename: "test_video",
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("Fake error")), "error");
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
          await getGcsFileDeletingTaskMetadata(SPANNER_DATABASE, "test_video"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 0,
              },
              GET_GCS_FILE_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "GcsFileDeletingTask",
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
            insertGcsFileDeletingTaskStatement("test_video", "", 0, 1000, 1000),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessGcsFileDeletingTaskHandler(
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
          await getGcsFileDeletingTaskMetadata(SPANNER_DATABASE, "test_video"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskRetryCount: 1,
                gcsFileDeletingTaskExecutionTimeMs: 301000,
              },
              GET_GCS_FILE_DELETING_TASK_METADATA_ROW,
            ),
          ]),
          "GcsFileDeletingTask",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
