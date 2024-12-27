import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteVideoContainerSyncingTaskStatement,
  insertVideoContainerSyncingTaskStatement,
} from "../db/sql";
import { ListVideoContainerSyncingTasksHandler } from "./list_video_container_syncing_tasks_handler";
import { LIST_VIDEO_CONTAINER_SYNCING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListVideoContainerSyncingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerSyncingTaskStatement("container1", 1, 100, 0),
            insertVideoContainerSyncingTaskStatement("container2", 2, 0, 0),
            insertVideoContainerSyncingTaskStatement("container3", 3, 1000, 0),
          ]);
          await transaction.commit();
        });
        let handler = new ListVideoContainerSyncingTasksHandler(
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
            deleteVideoContainerSyncingTaskStatement("container1", 1),
            deleteVideoContainerSyncingTaskStatement("container2", 2),
            deleteVideoContainerSyncingTaskStatement("container3", 3),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
