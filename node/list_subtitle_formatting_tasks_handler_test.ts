import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteSubtitleFormattingTaskStatement,
  insertSubtitleFormattingTaskStatement,
} from "../db/sql";
import { ListSubtitleFormattingTasksHandler } from "./list_subtitle_formatting_tasks_handler";
import { LIST_SUBTITLE_FORMATTING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListSubtitleFormattingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertSubtitleFormattingTaskStatement(
              "container1",
              "file1",
              100,
              0,
            ),
            insertSubtitleFormattingTaskStatement("container2", "file2", 0, 0),
            insertSubtitleFormattingTaskStatement(
              "container3",
              "file3",
              1000,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ListSubtitleFormattingTasksHandler(
          SPANNER_DATABASE,
          () => 100,
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              tasks: [
                { containerId: "container2", gcsFilename: "file2" },
                { containerId: "container1", gcsFilename: "file1" },
              ],
            },
            LIST_SUBTITLE_FORMATTING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteSubtitleFormattingTaskStatement("container1", "file1"),
            deleteSubtitleFormattingTaskStatement("container2", "file2"),
            deleteSubtitleFormattingTaskStatement("container3", "file3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
