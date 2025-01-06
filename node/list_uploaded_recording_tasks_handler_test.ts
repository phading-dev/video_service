import { SPANNER_DATABASE } from "../common/spanner_database";
import { insertUploadedRecordingTaskStatement, deleteUploadedRecordingTaskStatement } from "../db/sql";
import { ListUploadedRecordingTasksHandler } from "./list_uploaded_recording_tasks_handler";
import { LIST_UPLOADED_RECORDING_TASKS_RESPONSE } from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ListUploadedRecordingTasksHandlerTest",
  cases: [
    {
      name: "Default",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertUploadedRecordingTaskStatement(
              "file1",
              {
                gcsFilename: "file1",
                accountId: "account1",
                totalBytes: 1200,
              },
              100,
              0,
            ),
            insertUploadedRecordingTaskStatement(
              "file2",
              {
                gcsFilename: "file2",
                accountId: "account2",
                totalBytes: 2400,
              },
              0,
              0,
            ),
            insertUploadedRecordingTaskStatement(
              "file3",
              {
                gcsFilename: "file3",
                accountId: "account3",
                totalBytes: 3600,
              },
              1000,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ListUploadedRecordingTasksHandler(
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
                  gcsFilename: "file2",
                  accountId: "account2",
                  totalBytes: 2400,
                },
                {
                  gcsFilename: "file1",
                  accountId: "account1",
                  totalBytes: 1200,
                },
              ],
            },
            LIST_UPLOADED_RECORDING_TASKS_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteUploadedRecordingTaskStatement("file1"),
            deleteUploadedRecordingTaskStatement("file2"),
            deleteUploadedRecordingTaskStatement("file3"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});