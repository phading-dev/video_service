import "../local/env";
import { ENV_VARS } from "../env_vars";
import { DirectoryStreamUploader } from "./r2_directory_stream_uploader";
import { FileUploader } from "./r2_file_uploader";
import { S3_CLIENT, initS3Client } from "./s3_client";
import { DeleteObjectsCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { newInternalServerErrorError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { assertReject, assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { existsSync } from "fs";
import { mkdir, rm, writeFile } from "fs/promises";

TEST_RUNNER.run({
  name: "DirectoryStreamUploaderTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "WriteFilesAndFlush",
      execute: async () => {
        // Prepare
        await mkdir("temp_dir");
        let uploader = new DirectoryStreamUploader(
          await FileUploader.create(),
          "",
          "temp_dir",
          ENV_VARS.r2VideoBucketName,
          "remote_temp_dir",
        );

        // Execute
        uploader.start();
        for (let i = 0; i < 10; i++) {
          await writeFile(`temp_dir/file_${i}`, "some content");
        }
        let bytes = await uploader.flush();

        // Verify
        for (let i = 0; i < 10; i++) {
          assertThat(
            await (
              await S3_CLIENT.val.send(
                new GetObjectCommand({
                  Bucket: ENV_VARS.r2VideoBucketName,
                  Key: `remote_temp_dir/file_${i}`,
                }),
              )
            ).Body.transformToString(),
            eq("some content"),
            `Uploaded content ${i}`,
          );
        }
        assertThat(bytes, eq(120), "Uploaded bytes");
        assertThat(existsSync("temp_dir"), eq(false), "local temp dir");
      },
      tearDown: async () => {
        await rm("temp_dir", { recursive: true, force: true });
        await S3_CLIENT.val.send(
          new DeleteObjectsCommand({
            Bucket: ENV_VARS.r2VideoBucketName,
            Delete: {
              Objects: Array.from({ length: 10 }, (_, i) => ({
                Key: `remote_temp_dir/file_${i}`,
              })),
            },
          }),
        );
      },
    },
    {
      name: "WriteFilesAndFlushWithErrors",
      execute: async () => {
        // Prepare
        await mkdir("temp_dir");
        let uploaderMock = new (class extends FileUploader {
          public error?: Error;
          public constructor() {
            super(undefined, undefined, undefined);
          }
          public async upload(): Promise<void> {
            if (this.error) {
              throw this.error;
            }
          }
        })();
        let uploader = new DirectoryStreamUploader(
          uploaderMock,
          "",
          "temp_dir",
          ENV_VARS.r2VideoBucketName,
          "remote_temp_dir",
        );

        // Execute
        uploader.start();
        for (let i = 0; i < 10; i++) {
          await writeFile(`temp_dir/file_${i}`, "some content");
          if (i === 5) {
            uploaderMock.error = new Error("fake error");
          }
        }
        let error = await assertReject(uploader.flush());

        // Verify
        assertThat(
          error,
          eqHttpError(
            newInternalServerErrorError("Failed to upload the following files"),
          ),
          "error",
        );
        assertThat(existsSync("temp_dir"), eq(false), "local temp dir");
      },
      tearDown: async () => {
        await rm("temp_dir", { recursive: true, force: true });
      },
    },
  ],
});
