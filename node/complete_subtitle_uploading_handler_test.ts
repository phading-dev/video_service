import "../local/env";
import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_SUBTITLE_FORMATTING_TASK_ROW,
  GET_UPLOADED_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteSubtitleFormattingTaskStatement,
  deleteUploadedRecordingTaskStatement,
  deleteVideoContainerStatement,
  getSubtitleFormattingTask,
  getUploadedRecordingTask,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { CompleteSubtitleUploadingHandler } from "./complete_subtitle_uploading_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

let ZIP_FILE_SIZE = 1062;

async function createUploadSessionUrl(): Promise<string> {
  return CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
    ENV_VARS.gcsVideoBucketName,
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
              data: {
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
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
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
          await getUploadedRecordingTask(SPANNER_DATABASE, {
            uploadedRecordingTaskGcsFilenameEq: "test_subs",
          }),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskGcsFilename: "test_subs",
                uploadedRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: ZIP_FILE_SIZE,
                },
                uploadedRecordingTaskRetryCount: 0,
                uploadedRecordingTaskExecutionTimeMs: 1000,
                uploadedRecordingTaskCreatedTimeMs: 1000,
              },
              GET_UPLOADED_RECORDING_TASK_ROW,
            ),
          ]),
          "uploadedRecordingTasks",
        );
        assertThat(
          await getSubtitleFormattingTask(SPANNER_DATABASE, {
            subtitleFormattingTaskContainerIdEq: "container1",
            subtitleFormattingTaskGcsFilenameEq: "test_subs",
          }),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "test_subs",
                subtitleFormattingTaskRetryCount: 0,
                subtitleFormattingTaskExecutionTimeMs: 1000,
                subtitleFormattingTaskCreatedTimeMs: 1000,
              },
              GET_SUBTITLE_FORMATTING_TASK_ROW,
            ),
          ]),
          "subtitleFormattingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement({
              videoContainerContainerIdEq: "container1",
            }),
            deleteSubtitleFormattingTaskStatement({
              subtitleFormattingTaskContainerIdEq: "container1",
              subtitleFormattingTaskGcsFilenameEq: "test_subs",
            }),
            deleteUploadedRecordingTaskStatement({
              uploadedRecordingTaskGcsFilenameEq: "test_subs",
            }),
          ]);
          await transaction.commit();
        });
        await CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
          ENV_VARS.gcsVideoBucketName,
          "test_subs",
        );
      },
    },
  ],
});
