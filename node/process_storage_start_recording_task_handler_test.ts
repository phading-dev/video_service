import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_STORAGE_START_RECORDING_TASK_METADATA_ROW,
  deleteStorageStartRecordingTaskStatement,
  getStorageStartRecordingTaskMetadata,
  insertStorageStartRecordingTaskStatement,
  listPendingStorageStartRecordingTasks,
} from "../db/sql";
import { ProcessStorageStartRecordingTaskHandler } from "./process_storage_start_recording_task_handler";
import {
  RECORD_STORAGE_START,
  RECORD_STORAGE_START_REQUEST_BODY,
} from "@phading/meter_service_interface/show/node/publisher/interface";
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
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "dir1",
      }),
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
            insertStorageStartRecordingTaskStatement({
              r2Dirname: "dir1",
              payload: {
                accountId: "account1",
                totalBytes: 1204,
                startTimeMs: 900,
              },
              retryCount: 0,
              executionTimeMs: 100,
              createdTimeMs: 100,
            }),
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
        await handler.processTask("", {
          r2Dirname: "dir1",
          accountId: "account1",
          totalBytes: 1204,
          startTimeMs: 900,
        });

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
          await listPendingStorageStartRecordingTasks(SPANNER_DATABASE, {
            storageStartRecordingTaskExecutionTimeMsLe: 1000000,
          }),
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
            insertStorageStartRecordingTaskStatement({
              r2Dirname: "dir1",
              payload: {
                accountId: "account1",
                totalBytes: 1204,
                startTimeMs: 900,
              },
              retryCount: 0,
              executionTimeMs: 100,
              createdTimeMs: 100,
            }),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        clientMock.error = new Error("fake error");
        let handler = new ProcessStorageStartRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            r2Dirname: "dir1",
            accountId: "account1",
            totalBytes: 1204,
            startTimeMs: 900,
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("fake error")), "error");
        assertThat(
          await getStorageStartRecordingTaskMetadata(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "dir1",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 100,
              },
              GET_STORAGE_START_RECORDING_TASK_METADATA_ROW,
            ),
          ]),
          "Storage start recording task",
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
            insertStorageStartRecordingTaskStatement({
              r2Dirname: "dir1",
              payload: {
                accountId: "account1",
                totalBytes: 1204,
                startTimeMs: 900,
              },
              retryCount: 0,
              executionTimeMs: 100,
              createdTimeMs: 100,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new ProcessStorageStartRecordingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", { r2Dirname: "dir1" });

        // Verify
        assertThat(
          await getStorageStartRecordingTaskMetadata(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "dir1",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskRetryCount: 1,
                storageStartRecordingTaskExecutionTimeMs: 301000,
              },
              GET_STORAGE_START_RECORDING_TASK_METADATA_ROW,
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
