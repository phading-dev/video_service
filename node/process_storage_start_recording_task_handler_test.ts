import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  LIST_STORAGE_START_RECORDING_TASKS_ROW,
  deleteStorageStartRecordingTaskStatement,
  insertStorageStartRecordingTaskStatement,
  listStorageStartRecordingTasks,
} from "../db/sql";
import { ProcessStorageStartRecordingTaskHandler } from "./process_storage_start_recording_task_handler";
import {
  RECORD_STORAGE_START,
  RECORD_STORAGE_START_REQUEST_BODY,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteStorageStartRecordingTaskStatement("dir1"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "ProcessStorageStartRecordingTaskHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertStorageStartRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                totalBytes: 1204,
                startTimeMs: 900,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessStorageStartRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          r2Dirname: "dir1",
          accountId: "account1",
          totalBytes: 1204,
          startTimeMs: 900,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          clientMock.request.descriptor,
          eq(RECORD_STORAGE_START),
          "RC",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              name: "dir1",
              accountId: "account1",
              storageBytes: 1204,
              storageStartMs: 900,
            },
            RECORD_STORAGE_START_REQUEST_BODY,
          ),
          "RC body",
        );
        assertThat(
          await listStorageStartRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "Storage start recording task",
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
            insertStorageStartRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                totalBytes: 1204,
                startTimeMs: 900,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessStorageStartRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );
        handler.interfereFn = () => {
          throw new Error("Fake error");
        };

        // Execute
        handler.handle("", {
          r2Dirname: "dir1",
          accountId: "account1",
          totalBytes: 1204,
          startTimeMs: 900,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await listStorageStartRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskPayload: {
                  r2Dirname: "dir1",
                  accountId: "account1",
                  totalBytes: 1204,
                  startTimeMs: 900,
                },
                storageStartRecordingTaskExecutionTimeMs: 301000,
              },
              LIST_STORAGE_START_RECORDING_TASKS_ROW,
            ),
          ]),
          "Storage start recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
