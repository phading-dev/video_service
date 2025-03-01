import "../local/env";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  checkGcsFile,
  deleteGcsFileStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { StartMediaUploadingHandler } from "./start_media_uploading_handler";
import { assertThat, containStr, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "StartMediaUploadingHandlerTest",
  cases: [
    {
      name: "Start",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
            }),
          ]);
          await transaction.commit();
        });
        let id = 0;
        let handler = new StartMediaUploadingHandler(
          new StartResumableUploadingHandler(
            SPANNER_DATABASE,
            CLOUD_STORAGE_CLIENT,
            () => `uuid${id++}`,
            "media",
            (data) => data.processing?.media?.uploading,
            (data, uploading) => (data.processing = { media: { uploading } }),
          ),
        );

        // Execute
        let response = await handler.handle("container1", {
          containerId: "container1",
          fileType: "mp4",
          contentLength: 123,
        });

        // Verify
        assertThat(
          response.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${ENV_VARS.gcsVideoBucketName}/o?uploadType=resumable&name=uuid0`,
          ),
          "response.uploadSessionUrl",
        );
        assertThat(response.byteOffset, eq(0), "response.byteOffset");
        let videoContainer = (
          await getVideoContainer(SPANNER_DATABASE, "container1")
        )[0].videoContainerData;
        assertThat(
          videoContainer.processing?.media?.uploading?.gcsFilename,
          eq("uuid0"),
          "gcsFilename",
        );
        assertThat(
          videoContainer.processing?.media?.uploading?.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${ENV_VARS.gcsVideoBucketName}/o?uploadType=resumable&name=uuid0`,
          ),
          "uploadSessionUrl",
        );
        assertThat(
          videoContainer.processing?.media?.uploading?.contentLength,
          eq(123),
          "contentLength",
        );
        assertThat(
          videoContainer.processing?.media?.uploading?.contentType,
          eq("video/mp4"),
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
