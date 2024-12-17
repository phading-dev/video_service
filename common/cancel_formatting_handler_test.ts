import {
  GET_GCS_FILE_DELETE_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeleteTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeleteTasks,
  getMediaFormattingTasks,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
} from "../db/sql";
import { CancelFormattingHandler } from "./cancel_formatting_handler";
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
      deleteMediaFormattingTaskStatement("container1", "test_video"),
      deleteGcsFileDeleteTaskStatement("test_video"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "CancelFormattingHandlerTest",
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
                  formatting: {
                    gcsFilename: "test_video",
                  },
                },
              },
            }),
            insertMediaFormattingTaskStatement(
              "container1",
              "test_video",
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new CancelFormattingHandler(
          SPANNER_DATABASE,
          () => 1000,
          "media",
          (data) => data.processing?.media?.formatting,
          (containerId, gcsFilename) =>
            deleteMediaFormattingTaskStatement(containerId, gcsFilename),
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
          await getMediaFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "mediaFormattingTasks",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "test_video",
                gcsFileDeleteTaskPayload: {},
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
      name: "NotInFormattingState",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {}),
          ]);
          await transaction.commit();
        });
        let handler = new CancelFormattingHandler(
          SPANNER_DATABASE,
          () => 1000,
          "media",
          (data) => data.processing?.media?.formatting,
          (containerId, gcsFilename) =>
            deleteMediaFormattingTaskStatement(containerId, gcsFilename),
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newBadRequestError("is not in media formatting state")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});