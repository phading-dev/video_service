import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  GCS_VIDEO_LOCAL_DIR,
  GCS_VIDEO_REMOTE_BUCKET,
} from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_SUBTITLE_FORMATTING_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  getSubtitleFormattingTasks,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CompleteSubtitleUploadingHandler } from "./complete_subtitle_uploading_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { spawnSync } from "child_process";
import { createReadStream } from "fs";

let ZIP_FILE_SIZE = 1062;

async function createUploadSessionUrl(): Promise<string> {
  return CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
    GCS_VIDEO_REMOTE_BUCKET,
    "test_subs",
    "application/zip",
    ZIP_FILE_SIZE,
  );
}

async function uploadZip(uploadSessionUrl: string): Promise<void> {
  await axios.put(
    uploadSessionUrl,
    createReadStream("test_data/two_subs.zip"),
    {
      headers: {
        "Content-Length": ZIP_FILE_SIZE,
      },
    },
  );
  // Wait for GCS to catch up.
  await new Promise((resolve) => setTimeout(resolve, 1000));
}

TEST_RUNNER.run({
  name: "CompleteSubtitleUploadingHandlerTest",
  cases: [
    {
      name: "Complete",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadZip(uploadSessionUrl);
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              processing: {
                subtitle: {
                  uploading: {
                    gcsFilename: "test_subs",
                    uploadSessionUrl,
                    contentLength: ZIP_FILE_SIZE,
                    contentType: "application/zip",
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CompleteSubtitleUploadingHandler(
          (
            kind,
            getUploadingState,
            saveFormattingState,
            insertFormattingTaskStatement,
          ) =>
            new CompleteResumableUploadingHandler(
              SPANNER_DATABASE,
              CLOUD_STORAGE_CLIENT,
              () => 1000,
              kind,
              getUploadingState,
              saveFormattingState,
              insertFormattingTaskStatement,
            ),
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          uploadSessionUrl,
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  processing: {
                    subtitle: {
                      formatting: {
                        gcsFilename: "test_subs",
                      },
                    },
                  },
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await getSubtitleFormattingTasks(SPANNER_DATABASE, 100000),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "test_subs",
                subtitleFormattingTaskExecutionTimestamp: 1000,
              },
              GET_SUBTITLE_FORMATTING_TASKS_ROW,
            ),
          ]),
          "subtitleFormattingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteSubtitleFormattingTaskStatement("container1", "test_subs"),
          ]);
          await transaction.commit();
        });
        spawnSync("rm", ["-f", `${GCS_VIDEO_LOCAL_DIR}/test_subs`], {
          stdio: "inherit",
        });
      },
    },
  ],
});
