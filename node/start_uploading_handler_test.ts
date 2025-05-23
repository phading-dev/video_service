import "../local/env";
import axios from "axios";
import { CLOUD_STORAGE_CLIENT } from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteGcsFileStatement,
  deleteVideoContainerStatement,
  getGcsFile,
  getVideoContainer,
  insertVideoContainerStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { StartUploadingHandler } from "./start_uploading_handler";
import { newBadRequestError, newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import {
  assert,
  assertReject,
  assertThat,
  containStr,
  eq,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

let VIDEO_FILE_SIZE = 18328570;

async function insertVideoContainer(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        data: {},
      }),
    ]);
    await transaction.commit();
  });
}

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteGcsFileStatement({ gcsFileFilenameEq: "uuid0" }),
      deleteGcsFileStatement({ gcsFileFilenameEq: "uuid1" }),
    ]);
    await transaction.commit();
  });
  await CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
    ENV_VARS.gcsVideoBucketName,
    "uuid0",
  );
}

TEST_RUNNER.run({
  name: "StartUploadingHandlerTest",
  cases: [
    {
      name: "Start_UploadOneChunkAndStart",
      execute: async () => {
        // Prepare
        await insertVideoContainer();
        let id = 0;
        let handler = new StartUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => `uuid${id++}`,
        );

        // Execute
        let response = await handler.handle("", {
          containerId: "container1",
          contentLength: VIDEO_FILE_SIZE,
          fileExt: "mp4",
          md5: "md5string",
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
          videoContainer.processing?.uploading?.gcsFilename,
          eq("uuid0"),
          "gcsFilename",
        );
        assertThat(
          videoContainer.processing?.uploading?.uploadSessionUrl,
          containStr(
            `https://storage.googleapis.com/upload/storage/v1/b/${ENV_VARS.gcsVideoBucketName}/o?uploadType=resumable&name=uuid0`,
          ),
          "uploadSessionUrl",
        );
        assertThat(
          videoContainer.processing?.uploading?.contentLength,
          eq(VIDEO_FILE_SIZE),
          "contentLength",
        );
        assertThat(
          videoContainer.processing?.uploading?.fileExt,
          eq("mp4"),
          "fileExt",
        );
        assertThat(
          videoContainer.processing?.uploading?.md5,
          eq("md5string"),
          "md5",
        );
        assertThat(
          (await getGcsFile(SPANNER_DATABASE, { gcsFileFilenameEq: "uuid0" }))
            .length,
          eq(1),
          "GCS file",
        );

        // Prepare
        let uploadSessionUrl = response.uploadSessionUrl;
        let body = createReadStream("test_data/video_only.mp4", {
          start: 0,
          end: 256 * 1024,
        });
        try {
          await axios.put(uploadSessionUrl, body, {
            headers: {
              "Content-Length": 256 * 1024,
              "Content-Range": `bytes 0-${256 * 1024 - 1}/${VIDEO_FILE_SIZE}`,
            },
          });
        } catch (e) {
          assertThat(e.status, eq(308), "partial upload status");
        }
        // Wait for GCS to catch up.
        await new Promise<void>((resolve) => setTimeout(resolve, 1000));

        // Execute
        response = await handler.handle("", {
          containerId: "container1",
          contentLength: VIDEO_FILE_SIZE,
          fileExt: "mp4",
          md5: "md5string",
        });

        // Verify
        assertThat(
          response.uploadSessionUrl,
          eq(uploadSessionUrl),
          "response.uploadSessionUrl 2",
        );
        assertThat(
          response.byteOffset,
          eq(256 * 1024),
          "response.byteOffset 2",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "CancelAndRestart",
      execute: async () => {
        // Prepare
        await insertVideoContainer();
        let id = 0;
        let handler = new StartUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => `uuid${id++}`,
        );
        let { uploadSessionUrl } = await handler.handle("", {
          containerId: "container1",
          contentLength: VIDEO_FILE_SIZE,
          fileExt: "mp4",
          md5: "md5string",
        });

        // Exeucte
        CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
          ENV_VARS.gcsVideoBucketName,
          "uuid0",
          uploadSessionUrl,
        );
        let response = await handler.handle("", {
          containerId: "container1",
          contentLength: VIDEO_FILE_SIZE,
          fileExt: "mp4",
          md5: "md5string",
        });

        // Verify
        assert(
          response.uploadSessionUrl !== uploadSessionUrl,
          `new uploadSessionUrl not match`,
          `matched ${uploadSessionUrl}`,
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "UrlChangedAfterRestarted",
      execute: async () => {
        // Prepare
        await insertVideoContainer();
        let id = 0;
        let handler = new StartUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => `uuid${id++}`,
        );
        let { uploadSessionUrl } = await handler.handle("", {
          containerId: "container1",
          contentLength: VIDEO_FILE_SIZE,
          fileExt: "mp4",
          md5: "md5string",
        });
        CLOUD_STORAGE_CLIENT.deleteFileAndCancelUpload(
          ENV_VARS.gcsVideoBucketName,
          "uuid0",
          uploadSessionUrl,
        );

        handler.interfereFn = async () => {
          await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
            await transaction.batchUpdate([
              updateVideoContainerStatement({
                videoContainerContainerIdEq: "container1",
                setData: {
                  processing: {
                    uploading: {
                      gcsFilename: "uuid0",
                      uploadSessionUrl: "some_url",
                      contentLength: VIDEO_FILE_SIZE,
                      fileExt: "mp4",
                      md5: "md5string",
                    },
                  },
                },
              }),
            ]);
            await transaction.commit();
          });
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            contentLength: VIDEO_FILE_SIZE,
            fileExt: "mp4",
            md5: "md5string",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newConflictError("upload session URL has been changed")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInUploadingState",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                processing: {
                  mediaFormatting: {},
                },
              },
            }),
          ]);
          await transaction.commit();
        });
        let id = 0;
        let handler = new StartUploadingHandler(
          SPANNER_DATABASE,
          CLOUD_STORAGE_CLIENT,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            contentLength: VIDEO_FILE_SIZE,
            fileExt: "mp4",
            md5: "md5string",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newBadRequestError(
              "other processing state than uploading state",
            ),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
