import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_GCS_FILE_DELETING_TASKS_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  listGcsFileDeletingTasks,
  listMediaFormattingTasks,
} from "../db/sql";
import { CancelMediaFormattingHandler } from "./cancel_media_formatting_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "CancelMediaFormattingHandlerTest",
  cases: [
    {
      name: "Cancel",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
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
        let handler = new CancelMediaFormattingHandler(
          (kind, getFormattingState, deleteFormattingTaskStatement) =>
            new CancelFormattingHandler(
              SPANNER_DATABASE,
              () => 1000,
              kind,
              getFormattingState,
              deleteFormattingTaskStatement,
            ),
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
                videoContainerData: {
                  containerId: "container1",
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await listMediaFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "mediaFormattingTasks",
        );
        assertThat(
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "test_video",
                gcsFileDeletingTaskUploadSessionUrl: "",
                gcsFileDeletingTaskExecutionTimeMs: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
            ),
          ]),
          "gcsFileDeletingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteMediaFormattingTaskStatement("container1", "test_video"),
            deleteGcsFileDeletingTaskStatement("test_video"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
