import "../local/env";
import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_GCS_KEY_DELETING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsKeyDeletingTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteVideoContainerStatement,
  getGcsKeyDeletingTask,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  listPendingMediaFormattingTasks,
} from "../db/sql";
import { CancelMediaFormattingHandler } from "./cancel_media_formatting_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "CancelMediaFormattingHandlerTest",
  cases: [
    {
      name: "Cancel",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                processing: {
                  mediaFormatting: {
                    gcsFilename: "test_video",
                  },
                },
              },
            }),
            insertMediaFormattingTaskStatement({
              containerId: "container1",
              gcsFilename: "test_video",
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CancelMediaFormattingHandler(
          (kind, getFormattingState, deleteFormattingTaskStatement) =>
            new CancelFormattingHandler(
              SPANNER_DATABASE,
              () => 1000,
              kind,
              getFormattingState,
              deleteFormattingTaskStatement,
            ),
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerData: {},
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "mediaFormattingTasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "test_video",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "test_video",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "gcsKeyDeletingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement({
              videoContainerContainerIdEq: "container1",
            }),
            deleteMediaFormattingTaskStatement({
              mediaFormattingTaskContainerIdEq: "container1",
              mediaFormattingTaskGcsFilenameEq: "test_video",
            }),
            deleteGcsKeyDeletingTaskStatement({
              gcsKeyDeletingTaskKeyEq: "test_video",
            }),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
