import "../local/env";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  deleteGcsFileStatement,
  deleteVideoContainerStatement,
  getGcsFile,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
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
            insertVideoContainerStatement({
              containerId: "container1",
              data: {},
            }),
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
            `https://storage.googleapis.com/upload/storage/v1/b/${ENV_VARS.gcsVideoBucketName}/o?uploadType=resumable&name=uuid0`,
          ),
          "response.uploadSessionUrl",
        );
        assertThat(response.byteOffset, eq(0), "response.byteOffset");
        let videoContainer = (
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          })
        )[0].videoContainerData;
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.gcsFilename,
          eq("uuid0"),
          "gcsFilename",
        );
        assertThat(
          videoContainer.processing?.subtitle?.uploading?.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${ENV_VARS.gcsVideoBucketName}/o?uploadType=resumable&name=uuid0`,
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
          (await getGcsFile(SPANNER_DATABASE, { gcsFileFilenameEq: "uuid0" }))
            .length,
          eq(1),
          "GCS file",
        );
      },
      tearDown: async () => {
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            deleteVideoContainerStatement({
              videoContainerContainerIdEq: "container1",
            }),
            deleteGcsFileStatement({ gcsFileFilenameEq: "uuid0" }),
          ]);
          await transaction.commit();
        });
      },
    },
  ],
});
