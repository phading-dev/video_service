import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_GCS_FILE_DELETING_TASKS_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
  listGcsFileDeletingTasks,
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
            insertVideoContainerStatement("container1", {
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
                videoContainerData: {},
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "videoContainer",
        );
        assertThat(
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "test_subs",
                gcsFileDeletingTaskPayload: {
                  uploadSessionUrl: "uploadSessionUrl",
                },
                gcsFileDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
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
