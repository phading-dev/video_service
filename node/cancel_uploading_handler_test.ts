import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_GCS_UPLOAD_FILE_DELETING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsUploadFileDeletingTaskStatement,
  deleteVideoContainerStatement,
  getGcsUploadFileDeletingTask,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CancelUploadingHandler } from "./cancel_uploading_handler";
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
      deleteGcsUploadFileDeletingTaskStatement({
        gcsUploadFileDeletingTaskFilenameEq: "test_video",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "CancelUploadingHandlerTest",
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
                  uploading: {
                    gcsFilename: "test_video",
                    uploadSessionUrl: "uploadSessionUrl",
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CancelUploadingHandler(SPANNER_DATABASE, () => 1000);

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
          await getGcsUploadFileDeletingTask(SPANNER_DATABASE, {
            gcsUploadFileDeletingTaskFilenameEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsUploadFileDeletingTaskFilename: "test_video",
                gcsUploadFileDeletingTaskUploadSessionUrl: "uploadSessionUrl",
                gcsUploadFileDeletingTaskRetryCount: 0,
                gcsUploadFileDeletingTaskExecutionTimeMs: 1000,
                gcsUploadFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_UPLOAD_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcsUploadFileDeletingTasks",
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
        let handler = new CancelUploadingHandler(SPANNER_DATABASE, () => 1000);

        // Execute
        let error = await assertReject(
          handler.handle("", { containerId: "container1" }),
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
