import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteStorageStartRecordingTaskStatement,
  insertStorageStartRecordingTaskStatement,
} from "../db/sql";
import { ListStorageStartRecordingTasksHandler } from "./list_storage_start_recording_tasks_handler";
import { LIST_STORAGE_START_RECORDING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListStorageStartRecordingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertStorageStartRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                totalBytes: 1200,
                startTimeMs: 1000,
              },
              100,
              0,
            ),
            insertStorageStartRecordingTaskStatement(
              "dir2",
              {
                r2Dirname: "dir2",
                accountId: "account2",
                totalBytes: 2400,
                startTimeMs: 1000,
              },
              0,
              0,
            ),
            insertStorageStartRecordingTaskStatement(
              "dir3",
              {
                r2Dirname: "dir3",
                accountId: "account3",
                totalBytes: 3600,
                startTimeMs: 1000,
              },
              1000,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ListStorageStartRecordingTasksHandler(
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
                  totalBytes: 2400,
                  startTimeMs: 1000,
                },
                {
                  r2Dirname: "dir1",
                  accountId: "account1",
                  totalBytes: 1200,
                  startTimeMs: 1000,
                },
              ],
            },
            LIST_STORAGE_START_RECORDING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteStorageStartRecordingTaskStatement("dir1"),
            deleteStorageStartRecordingTaskStatement("dir2"),
            deleteStorageStartRecordingTaskStatement("dir3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
