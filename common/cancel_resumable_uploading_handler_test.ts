import {
  GET_GCS_FILE_DELETE_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeleteTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeleteTasks,
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
      deleteVideoContainerStatement("container1"),
      deleteGcsFileDeleteTaskStatement("test_video"),
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
            insertVideoContainerStatement("container1", {
              processing: {
                media: {
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
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {},
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "test_video",
                gcsFileDeleteTaskPayload: {
                  uploadSessionUrl: "uploadSessionUrl",
                },
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcsFileDeleteTasks",
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
            insertVideoContainerStatement("container1", {
              processing: {
                media: {
                  formatting: {},
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