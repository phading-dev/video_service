import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  GCS_VIDEO_LOCAL_DIR,
  GCS_VIDEO_REMOTE_BUCKET,
} from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_MEDIA_FORMATTING_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteMediaFormattingTaskStatement,
  deleteVideoContainerStatement,
  getMediaFormattingTasks,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CompleteMediaUploadingHandler } from "./complete_media_uploading_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { spawnSync } from "child_process";
import { createReadStream } from "fs";

let VIDEO_FILE_SIZE = 18328570;

async function createUploadSessionUrl(): Promise<string> {
  return CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
    GCS_VIDEO_REMOTE_BUCKET,
    "test_video",
    "video/mp4",
    VIDEO_FILE_SIZE,
  );
}

async function uploadVideo(uploadSessionUrl: string): Promise<void> {
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
  await new Promise((resolve) => setTimeout(resolve, 1000));
}

TEST_RUNNER.run({
  name: "CompleteMediaUploadingHandlerTest",
  cases: [
    {
      name: "Complete",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadVideo(uploadSessionUrl);
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              processing: {
                media: {
                  uploading: {
                    gcsFilename: "test_video",
                    uploadSessionUrl,
                    contentLength: VIDEO_FILE_SIZE,
                    contentType: "video/mp4",
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CompleteMediaUploadingHandler(
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
                    media: {
                      formatting: {
                        gcsFilename: "test_video",
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
          await getMediaFormattingTasks(SPANNER_DATABASE, 100000),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskContainerId: "container1",
                mediaFormattingTaskGcsFilename: "test_video",
                mediaFormattingTaskExecutionTimestamp: 1000,
              },
              GET_MEDIA_FORMATTING_TASKS_ROW,
            ),
          ]),
          "mediaFormattingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteMediaFormattingTaskStatement("container1", "test_video"),
          ]);
          await transaction.commit();
        });
        spawnSync("rm", ["-f", `${GCS_VIDEO_LOCAL_DIR}/test_video`], {
          stdio: "inherit",
        });
      },
    },
  ],
});