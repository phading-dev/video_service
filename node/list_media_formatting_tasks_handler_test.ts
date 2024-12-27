import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteMediaFormattingTaskStatement,
  insertMediaFormattingTaskStatement,
} from "../db/sql";
import { ListMediaFormattingTasksHandler } from "./list_media_formatting_tasks_handler";
import { LIST_MEDIA_FORMATTING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListMediaFormattingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertMediaFormattingTaskStatement("container1", "file1", 100, 0),
            insertMediaFormattingTaskStatement("container2", "file2", 0, 0),
            insertMediaFormattingTaskStatement("container3", "file3", 1000, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ListMediaFormattingTasksHandler(
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
            LIST_MEDIA_FORMATTING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteMediaFormattingTaskStatement("container1", "file1"),
            deleteMediaFormattingTaskStatement("container2", "file2"),
            deleteMediaFormattingTaskStatement("container3", "file3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
