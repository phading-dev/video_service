import "../local/env";
import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_GCS_FILE_DELETING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeletingTask,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CancelSubtitleUploadingHandler } from "./cancel_subtitle_uploading_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "CancelSubtitleUploadingHandlerTest",
  cases: [
    {
      name: "Cancel",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              processing: {
                subtitle: {
                  uploading: {
                    gcsFilename: "test_subs",
                    uploadSessionUrl: "uploadSessionUrl",
                  },
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CancelSubtitleUploadingHandler(
          (kind, getUploadingState) =>
            new CancelResumableUploadingHandler(
              SPANNER_DATABASE,
              () => 1000,
              kind,
              getUploadingState,
            ),
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  containerId: "container1",
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, "test_subs"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "test_subs",
                gcsFileDeletingTaskUploadSessionUrl: "uploadSessionUrl",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcsFileDeletingTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteGcsFileDeletingTaskStatement("test_subs"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
