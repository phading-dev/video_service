import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteStorageEndRecordingTaskStatement,
  insertStorageEndRecordingTaskStatement,
} from "../db/sql";
import { ListStorageEndRecordingTasksHandler } from "./list_storage_end_recording_tasks_handler";
import { LIST_STORAGE_END_RECORDING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListStorageEndRecordingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertStorageEndRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                endTimeMs: 2000,
              },
              100,
              0,
            ),
            insertStorageEndRecordingTaskStatement(
              "dir2",
              {
                r2Dirname: "dir2",
                accountId: "account2",
                endTimeMs: 2000,
              },
              0,
              0,
            ),
            insertStorageEndRecordingTaskStatement(
              "dir3",
              {
                r2Dirname: "dir3",
                accountId: "account3",
                endTimeMs: 2000,
              },
              1000,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ListStorageEndRecordingTasksHandler(
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
                {
                  r2Dirname: "dir2",
                  accountId: "account2",
                  endTimeMs: 2000,
                },
                {
                  r2Dirname: "dir1",
                  accountId: "account1",
                  endTimeMs: 2000,
                },
              ],
            },
            LIST_STORAGE_END_RECORDING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteStorageEndRecordingTaskStatement("dir1"),
            deleteStorageEndRecordingTaskStatement("dir2"),
            deleteStorageEndRecordingTaskStatement("dir3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
