import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_STORAGE_END_RECORDING_TASK_METADATA_ROW,
  deleteStorageEndRecordingTaskStatement,
  getStorageEndRecordingTaskMetadata,
  insertStorageEndRecordingTaskStatement,
  listPendingStorageEndRecordingTasks,
} from "../db/sql";
import { ProcessStorageEndRecordingTaskHandler } from "./process_storage_end_recording_task_handler";
import {
  RECORD_STORAGE_END,
  RECORD_STORAGE_END_REQUEST_BODY,
} from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
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
                accountId: "account1",
                endTimeMs: 900,
              },
              0,
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
        await handler.processTask("", {
          r2Dirname: "dir1",
          accountId: "account1",
          endTimeMs: 900,
        });

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
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, 1000000),
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
                accountId: "account1",
                endTimeMs: 900,
              },
              0,
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        clientMock.error = new Error("fake error");
        let handler = new ProcessStorageEndRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            r2Dirname: "dir1",
            accountId: "account1",
            endTimeMs: 900,
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("fake error")), "Error");
        assertThat(
          await getStorageEndRecordingTaskMetadata(SPANNER_DATABASE, "dir1"),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskRetryCount: 0,
                storageEndRecordingTaskExecutionTimeMs: 100,
              },
              GET_STORAGE_END_RECORDING_TASK_METADATA_ROW,
            ),
          ]),
          "Storage end recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ClaimTask",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertStorageEndRecordingTaskStatement(
              "dir1",
              {
                accountId: "account1",
                endTimeMs: 1000,
              },
              0,
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessStorageEndRecordingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", {
          r2Dirname: "dir1",
          accountId: "account1",
          endTimeMs: 1000,
        });

        // Verify
        assertThat(
          await getStorageEndRecordingTaskMetadata(SPANNER_DATABASE, "dir1"),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskRetryCount: 1,
                storageEndRecordingTaskExecutionTimeMs: 301000,
              },
              GET_STORAGE_END_RECORDING_TASK_METADATA_ROW,
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
