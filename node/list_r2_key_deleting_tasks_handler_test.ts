import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteR2KeyDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
} from "../db/sql";
import { ListR2KeyDeletingTasksHandler } from "./list_r2_key_deleting_tasks_handler";
import { LIST_R2_KEY_DELETING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListR2KeyDeletingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertR2KeyDeletingTaskStatement("file1", 100, 0),
            insertR2KeyDeletingTaskStatement("file2", 0, 0),
            insertR2KeyDeletingTaskStatement("file3", 1000, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ListR2KeyDeletingTasksHandler(
          SPANNER_DATABASE,
          () => 100,
        );

        // Execute
        let response = await handler.handle("prefix", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              tasks: [{ key: "file2" }, { key: "file1" }],
            },
            LIST_R2_KEY_DELETING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteR2KeyDeletingTaskStatement("file1"),
            deleteR2KeyDeletingTaskStatement("file2"),
            deleteR2KeyDeletingTaskStatement("file3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
