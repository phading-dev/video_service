import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  LIST_STORAGE_END_RECORDING_TASKS_ROW,
  deleteStorageEndRecordingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  listStorageEndRecordingTasks,
} from "../db/sql";
import { ProcessStorageEndRecordingTaskHandler } from "./process_storage_end_recording_task_handler";
import {
  RECORD_STORAGE_END,
  RECORD_STORAGE_END_REQUEST_BODY,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteStorageEndRecordingTaskStatement("dir1"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "ProcessStorageEndRecordingTaskHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertStorageEndRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                endTimeMs: 900,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessStorageEndRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          r2Dirname: "dir1",
          accountId: "account1",
          endTimeMs: 900,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(clientMock.request.descriptor, eq(RECORD_STORAGE_END), "RC");
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              name: "dir1",
              accountId: "account1",
              storageEndMs: 900,
            },
            RECORD_STORAGE_END_REQUEST_BODY,
          ),
          "RC body",
        );
        assertThat(
          await listStorageEndRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "Storage end recording task",
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
            insertStorageEndRecordingTaskStatement(
              "dir1",
              {
                r2Dirname: "dir1",
                accountId: "account1",
                endTimeMs: 900,
              },
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessStorageEndRecordingTaskHandler(
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
          endTimeMs: 900,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await listStorageEndRecordingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskPayload: {
                  r2Dirname: "dir1",
                  accountId: "account1",
                  endTimeMs: 900,
                },
                storageEndRecordingTaskExecutionTimeMs: 301000,
              },
              LIST_STORAGE_END_RECORDING_TASKS_ROW,
            ),
          ]),
          "Storage end recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
