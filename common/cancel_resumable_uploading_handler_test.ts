import "../local/env";
import {
  GET_GCS_FILE_DELETING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeletingTask,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CancelResumableUploadingHandler } from "./cancel_resumable_uploading_handler";
import { SPANNER_DATABASE } from "./spanner_database";
import { newBadRequestError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteGcsFileDeletingTaskStatement({
        gcsFileDeletingTaskFilenameEq: "test_video",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "CancelResumableUploadingHandlerTest",
  cases: [
    {
      name: "Cancel",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                processing: {
                  media: {
                    uploading: {
                      gcsFilename: "test_video",
                      uploadSessionUrl: "uploadSessionUrl",
                    },
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CancelResumableUploadingHandler(
          SPANNER_DATABASE,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
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
                videoContainerData: {},
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, {
            gcsFileDeletingTaskFilenameEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "test_video",
                gcsFileDeletingTaskUploadSessionUrl: "uploadSessionUrl",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcsFileDeletingTasks",
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
                  media: {
                    formatting: {},
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CancelResumableUploadingHandler(
          SPANNER_DATABASE,
          () => 1000,
          "media",
          (data) => data.processing?.media?.uploading,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", { containerId: "container1" }),
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
