import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  LIST_UPLOADED_RECORDING_TASKS_ROW,
  deleteUploadedRecordingTaskStatement,
  insertUploadedRecordingTaskStatement,
  listUploadedRecordingTasks,
} from "../db/sql";
import { ProcessUploadedRecordingTaskHandler } from "./process_uploaded_recording_task_handler";
import {
  RECORD_UPLOADED,
  RECORD_UPLOADED_REQUEST_BODY,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteUploadedRecordingTaskStatement("file1"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "ProcessUploadedRecordingTaskHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertUploadedRecordingTaskStatement(
              "file1",
              {
                gcsFilename: "file1",
                accountId: "account1",
                totalBytes: 1204,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessUploadedRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          gcsFilename: "file1",
          accountId: "account1",
          totalBytes: 1204,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(clientMock.request.descriptor, eq(RECORD_UPLOADED), "RC");
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              name: "file1",
              accountId: "account1",
              uploadedBytes: 1204,
            },
            RECORD_UPLOADED_REQUEST_BODY,
          ),
          "RC body",
        );
        assertThat(
          await listUploadedRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "Uploaded recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ProcessingFailed",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertUploadedRecordingTaskStatement(
              "file1",
              {
                gcsFilename: "file1",
                accountId: "account1",
                totalBytes: 1204,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessUploadedRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );
        handler.interfereFn = () => {
          throw new Error("Fake error");
        };

        // Execute
        handler.handle("", {
          gcsFilename: "file1",
          accountId: "account1",
          totalBytes: 1204,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await listUploadedRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskPayload: {
                  gcsFilename: "file1",
                  accountId: "account1",
                  totalBytes: 1204,
                },
                uploadedRecordingTaskExecutionTimeMs: 301000,
              },
              LIST_UPLOADED_RECORDING_TASKS_ROW,
            ),
          ]),
          "Uploaded recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
