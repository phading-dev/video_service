import "../local/env";
import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_MEDIA_FORMATTING_TASK_ROW,
  GET_SUBTITLE_FORMATTING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteMediaFormattingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteUploadedRecordingTaskStatement,
  deleteVideoContainerStatement,
  getMediaFormattingTask,
  getSubtitleFormattingTask,
  getVideoContainer,
  insertVideoContainerStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { CompleteUploadingHandler } from "./complete_uploading_handler";
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
    "test_file",
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

async function insertVideoContainer(
  uploadSessionUrl: string,
  fileExt: string,
): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        accountId: "account1",
        data: {
          processing: {
            uploading: {
              gcsFilename: "test_file",
              uploadSessionUrl,
              contentLength: VIDEO_FILE_SIZE,
              fileExt,
              md5: "test_md5",
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
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteMediaFormattingTaskStatement({
        mediaFormattingTaskContainerIdEq: "container1",
        mediaFormattingTaskGcsFilenameEq: "test_file",
      }),
      deleteSubtitleFormattingTaskStatement({
        subtitleFormattingTaskContainerIdEq: "container1",
        subtitleFormattingTaskGcsFilenameEq: "test_file",
      }),
      deleteUploadedRecordingTaskStatement({
        uploadedRecordingTaskGcsFilenameEq: "test_file",
      }),
    ]);
    await transaction.commit();
  });
  await CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
    ENV_VARS.gcsVideoBucketName,
    "test_file",
  );
}

TEST_RUNNER.run({
  name: "CompleteUploadingHandlerTest",
  cases: [
    {
      name: "CompleteMediaUploading",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadVideo(uploadSessionUrl);
        await insertVideoContainer(uploadSessionUrl, "mp4");
        let handler = new CompleteUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
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
                    mediaFormatting: {
                      gcsFilename: "test_file",
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
          await getMediaFormattingTask(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "test_file",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskContainerId: "container1",
                mediaFormattingTaskGcsFilename: "test_file",
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
      name: "CompleteSubtitleUploading",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await uploadVideo(uploadSessionUrl);
        await insertVideoContainer(uploadSessionUrl, "zip");
        let handler = new CompleteUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
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
                    subtitleFormatting: {
                      gcsFilename: "test_file",
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
          await getSubtitleFormattingTask(SPANNER_DATABASE, {
            subtitleFormattingTaskContainerIdEq: "container1",
            subtitleFormattingTaskGcsFilenameEq: "test_file",
          }),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "test_file",
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
        await cleanupAll();
      },
    },
    {
      name: "UploadIncomplete",
      execute: async () => {
        // Prepare
        let uploadSessionUrl = await createUploadSessionUrl();
        await insertVideoContainer(uploadSessionUrl, "mp4");
        let handler = new CompleteUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
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
        await insertVideoContainer(uploadSessionUrl, "mp4");
        let handler = new CompleteUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
        );
        handler.interfaceFn = async () => {
          await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
            await transaction.batchUpdate([
              updateVideoContainerStatement({
                videoContainerContainerIdEq: "container1",
                setData: {
                  processing: {
                    mediaFormatting: {
                      gcsFilename: "test_file",
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
          eqHttpError(newConflictError("is not in uploading state")),
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
              data: {
                processing: {
                  mediaFormatting: {},
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CompleteUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => 1000,
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
          eqHttpError(newBadRequestError("is not in uploading state")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
