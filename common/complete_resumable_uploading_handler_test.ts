import "../local/env";
import axios from "axios";
import {
  GET_MEDIA_FORMATTING_TASK_ROW,
  GET_UPLOADED_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteMediaFormattingTaskStatement,
  deleteUploadedRecordingTaskStatement,
  deleteVideoContainerStatement,
  getMediaFormattingTask,
  getUploadedRecordingTask,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { CLOUD_STORAGE_CLIENT } from "./cloud_storage_client";
import { CompleteResumableUploadingHandler } from "./complete_resumable_uploading_handler";
import { SPANNER_DATABASE } from "./spanner_database";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

let VIDEO_FILE_SIZE = 18328570;

async function createUploadSessionUrl(): Promise<string> {
  return CLOUD_STORAGE_CLIENT.createResumableUploadUrl(
    ENV_VARS.gcsVideoBucketName,
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

async function insertVideoContainer(uploadSessionUrl: string): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        accountId: "account1",
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
}

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteMediaFormattingTaskStatement("container1", "test_video"),
      deleteUploadedRecordingTaskStatement("test_video"),
    ]);
    await transaction.commit();
  });
  await CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
    ENV_VARS.gcsVideoBucketName,
    "test_video",
  );
}

TEST_RUNNER.run({
  name: "CompleteResumableUploadingHandlerTest",
  cases: [
    {
      name: "Complete",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadVideo(uploadSessionUrl);
        await insertVideoContainer(uploadSessionUrl);
        let handler = new CompleteResumableUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
          (data, formatting) => {
            data.processing = {
              media: {
                formatting,
              },
            };
          },
          (
            containerId,
            gcsFilename,
            retryCount,
            executionTimeMs,
            createdTimeMs,
          ) =>
            insertMediaFormattingTaskStatement(
              containerId,
              gcsFilename,
              retryCount,
              executionTimeMs,
              createdTimeMs,
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
          await getUploadedRecordingTask(SPANNER_DATABASE, "test_video"),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskGcsFilename: "test_video",
                uploadedRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: VIDEO_FILE_SIZE,
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
          await getMediaFormattingTask(
            SPANNER_DATABASE,
            "container1",
            "test_video",
          ),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskContainerId: "container1",
                mediaFormattingTaskGcsFilename: "test_video",
                mediaFormattingTaskRetryCount: 0,
                mediaFormattingTaskExecutionTimeMs: 1000,
                mediaFormattingTaskCreatedTimeMs: 1000,
              },
              GET_MEDIA_FORMATTING_TASK_ROW,
            ),
          ]),
          "mediaFormattingTasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "UploadIncomplete",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await insertVideoContainer(uploadSessionUrl);
        let handler = new CompleteResumableUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
          (data, formatting) => {
            data.processing = {
              media: {
                formatting,
              },
            };
          },
          (
            containerId,
            gcsFilename,
            retryCount,
            executionTimeMs,
            createdTimeMs,
          ) =>
            insertMediaFormattingTaskStatement(
              containerId,
              gcsFilename,
              retryCount,
              executionTimeMs,
              createdTimeMs,
            ),
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            uploadSessionUrl,
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newBadRequestError("has not finished uploading")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StateChangedWhileCompleting",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadVideo(uploadSessionUrl);
        await insertVideoContainer(uploadSessionUrl);
        let handler = new CompleteResumableUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
          (data, formatting) => {
            data.processing = {
              media: {
                formatting,
              },
            };
          },
          (
            containerId,
            gcsFilename,
            retryCount,
            executionTimeMs,
            createdTimeMs,
          ) =>
            insertMediaFormattingTaskStatement(
              containerId,
              gcsFilename,
              retryCount,
              executionTimeMs,
              createdTimeMs,
            ),
        );
        handler.interfaceFn = async () => {
          await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
            await transaction.batchUpdate([
              updateVideoContainerStatement({
                containerId: "container1",
                processing: {
                  media: {
                    formatting: {
                      gcsFilename: "test_video",
                    },
                  },
                },
              }),
            ]);
            await transaction.commit();
          });
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            uploadSessionUrl,
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newConflictError("is not in media uploading state")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInUploadingState",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              processing: {
                media: {
                  formatting: {},
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CompleteResumableUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
          (data, formatting) => {
            data.processing = {
              media: {
                formatting,
              },
            };
          },
          (
            containerId,
            gcsFilename,
            retryCount,
            executionTimeMs,
            createdTimeMs,
          ) =>
            insertMediaFormattingTaskStatement(
              containerId,
              gcsFilename,
              retryCount,
              executionTimeMs,
              createdTimeMs,
            ),
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            uploadSessionUrl: "some_url",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newBadRequestError("is not in media uploading state")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
