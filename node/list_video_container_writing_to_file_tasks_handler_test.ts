import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteVideoContainerWritingToFileTaskStatement,
  insertVideoContainerWritingToFileTaskStatement,
} from "../db/sql";
import { ListVideoContainerWritingToFileTasksHandler } from "./list_video_container_writing_to_file_tasks_handler";
import { LIST_VIDEO_CONTAINER_SYNCING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListVideoContainerWritingToFileTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerWritingToFileTaskStatement(
              "container1",
              1,
              100,
              0,
            ),
            insertVideoContainerWritingToFileTaskStatement(
              "container2",
              2,
              0,
              0,
            ),
            insertVideoContainerWritingToFileTaskStatement(
              "container3",
              3,
              1000,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ListVideoContainerWritingToFileTasksHandler(
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
                { containerId: "container2", version: 2 },
                { containerId: "container1", version: 1 },
              ],
            },
            LIST_VIDEO_CONTAINER_SYNCING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerWritingToFileTaskStatement("container1", 1),
            deleteVideoContainerWritingToFileTaskStatement("container2", 2),
            deleteVideoContainerWritingToFileTaskStatement("container3", 3),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
