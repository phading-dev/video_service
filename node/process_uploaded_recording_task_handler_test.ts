import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_UPLOADED_RECORDING_TASK_METADATA_ROW,
  deleteUploadedRecordingTaskStatement,
  getUploadedRecordingTaskMetadata,
  insertUploadedRecordingTaskStatement,
  listPendingUploadedRecordingTasks,
} from "../db/sql";
import { ProcessUploadedRecordingTaskHandler } from "./process_uploaded_recording_task_handler";
import {
  RECORD_UPLOADED,
  RECORD_UPLOADED_REQUEST_BODY,
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
                accountId: "account1",
                totalBytes: 1204,
              },
              0,
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
        await handler.processTask("", {
          gcsFilename: "file1",
          accountId: "account1",
          totalBytes: 1204,
        });

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
          await listPendingUploadedRecordingTasks(SPANNER_DATABASE, 1000000),
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
                accountId: "account1",
                totalBytes: 1204,
              },
              0,
              100,
              100,
            ),
          ]);
          await transaction.commit();
        });
        let clientMock = new NodeServiceClientMock();
        clientMock.error = new Error("Fake error");
        let handler = new ProcessUploadedRecordingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            gcsFilename: "file1",
            accountId: "account1",
            totalBytes: 1204,
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("Fake error")), "error");
        assertThat(
          await getUploadedRecordingTaskMetadata(SPANNER_DATABASE, "file1"),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskRetryCount: 0,
                uploadedRecordingTaskExecutionTimeMs: 100,
              },
              GET_UPLOADED_RECORDING_TASK_METADATA_ROW,
            ),
          ]),
          "Uploaded recording task",
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
            insertUploadedRecordingTaskStatement(
              "file1",
              {
                accountId: "account1",
                totalBytes: 2048,
              },
              0,
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
        await handler.claimTask("", {
          gcsFilename: "file1",
          accountId: "account1",
          totalBytes: 2048,
        });

        // Verify
        assertThat(
          await getUploadedRecordingTaskMetadata(SPANNER_DATABASE, "file1"),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskRetryCount: 1,
                uploadedRecordingTaskExecutionTimeMs: 301000,
              },
              GET_UPLOADED_RECORDING_TASK_METADATA_ROW,
            ),
          ]),
          "Claimed recording task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
