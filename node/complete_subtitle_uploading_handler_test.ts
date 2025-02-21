import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import { GCS_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_SUBTITLE_FORMATTING_TASKS_ROW,
  LIST_UPLOADED_RECORDING_TASKS_ROW,
  deleteSubtitleFormattingTaskStatement,
  deleteUploadedRecordingTaskStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
  listSubtitleFormattingTasks,
  listUploadedRecordingTasks,
} from "../db/sql";
import { CompleteSubtitleUploadingHandler } from "./complete_subtitle_uploading_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
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
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
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
                  containerId: "container1",
                  accountId: "account1",
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
          await listUploadedRecordingTasks(SPANNER_DATABASE, 100000),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskPayload: {
                  gcsFilename: "test_subs",
                  accountId: "account1",
                  totalBytes: ZIP_FILE_SIZE,
                },
                uploadedRecordingTaskExecutionTimeMs: 1000,
              },
              LIST_UPLOADED_RECORDING_TASKS_ROW,
            ),
          ]),
          "uploadedRecordingTasks",
        );
        assertThat(
          await listSubtitleFormattingTasks(SPANNER_DATABASE, 100000),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "test_subs",
                subtitleFormattingTaskExecutionTimeMs: 1000,
              },
              LIST_SUBTITLE_FORMATTING_TASKS_ROW,
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
            deleteUploadedRecordingTaskStatement("test_subs"),
          ]);
          await transaction.commit();
        });
        await CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
          GCS_VIDEO_REMOTE_BUCKET,
          "test_subs",
        );
      },
    },
  ],
});
