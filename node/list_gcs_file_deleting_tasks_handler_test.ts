import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteGcsFileDeletingTaskStatement,
  insertGcsFileDeletingTaskStatement,
} from "../db/sql";
import { ListGcsFileDeletingTasksHandler } from "./list_gcs_file_deleting_tasks_handler";
import { LIST_GCS_FILE_DELETING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListGcsFileDeletingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertGcsFileDeletingTaskStatement("file1", "url1", 100, 0),
            insertGcsFileDeletingTaskStatement("file2", "", 0, 0),
            insertGcsFileDeletingTaskStatement("file3", "url1", 1000, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ListGcsFileDeletingTasksHandler(
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
                { gcsFilename: "file2", uploadSessionUrl: "" },
                { gcsFilename: "file1", uploadSessionUrl: "url1" },
              ],
            },
            LIST_GCS_FILE_DELETING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteGcsFileDeletingTaskStatement("file1"),
            deleteGcsFileDeletingTaskStatement("file2"),
            deleteGcsFileDeletingTaskStatement("file3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
