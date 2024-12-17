import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { GCS_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  checkGcsFile,
  deleteGcsFileStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { StartSubtitleUploadingHandler } from "./start_subtitle_uploading_handler";
import { assertThat, containStr, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "StartSubtitleUploadingHandlerTest",
  cases: [
    {
      name: "Start",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {}),
          ]);
          await transaction.commit();
        });
        let id = 0;
        let handler = new StartSubtitleUploadingHandler(
          new StartResumableUploadingHandler(
            SPANNER_DATABASE,
            CLOUD_STORAGE_CLIENT,
            () => `uuid${id++}`,
            "subtitle",
            (data) => data.processing?.subtitle?.uploading,
            (data, uploading) =>
              (data.processing = { subtitle: { uploading } }),
          ),
        );

        // Execute
        let response = await handler.handle("container1", {
          containerId: "container1",
          fileType: "zip",
          contentLength: 123,
        });

        // Verify
        assertThat(
          response.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${GCS_VIDEO_REMOTE_BUCKET}/o?uploadType=resumable&name=uuid0`,
          ),
          "response.uploadSessionUrl",
        );
        assertThat(response.byteOffset, eq(0), "response.byteOffset");
        let videoContainer = (
          await getVideoContainer(SPANNER_DATABASE, "container1")
        )[0].videoContainerData;
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.gcsFilename,
          eq("uuid0"),
          "gcsFilename",
        );
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${GCS_VIDEO_REMOTE_BUCKET}/o?uploadType=resumable&name=uuid0`,
          ),
          "uploadSessionUrl",
        );
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.contentLength,
          eq(123),
          "contentLength",
        );
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.contentType,
          eq("application/zip"),
          "contentType",
        );
        assertThat(
          (await checkGcsFile(SPANNER_DATABASE, "uuid0")).length,
          eq(1),
          "GCS file",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement("container1"),
            deleteGcsFileStatement("uuid0"),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
