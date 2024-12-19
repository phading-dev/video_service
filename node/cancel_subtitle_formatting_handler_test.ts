import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_GCS_FILE_DELETE_TASKS_ROW,
  deleteGcsFileDeleteTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertSubtitleFormattingTaskStatement,
  insertVideoContainerStatement,
  listGcsFileDeleteTasks,
  listMediaFormattingTasks,
} from "../db/sql";
import { CancelSubtitleFormattingHandler } from "./cancel_subtitle_formatting_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "CancelSubtitleFormattingHandlerTest",
  cases: [
    {
      name: "Cancel",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              processing: {
                subtitle: {
                  formatting: {
                    gcsFilename: "test_subs",
                  },
                },
              },
            }),
            insertSubtitleFormattingTaskStatement(
              "container1",
              "test_subs",
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new CancelSubtitleFormattingHandler(
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
                videoContainerData: {},
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
          await listGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "test_subs",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcsFileDeleteTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteMediaFormattingTaskStatement("container1", "test_subs"),
            deleteGcsFileDeleteTaskStatement("test_subs"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
