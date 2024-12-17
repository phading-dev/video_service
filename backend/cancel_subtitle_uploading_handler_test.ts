import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_GCS_FILE_DELETE_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeleteTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeleteTasks,
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
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "test_subs",
                gcsFileDeleteTaskPayload: {
                  uploadSessionUrl: "uploadSessionUrl",
                },
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcsFileDeleteTasks",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteGcsFileDeleteTaskStatement("test_subs"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
