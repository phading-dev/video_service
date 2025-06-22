import "../local/env";
import { ENV_VARS } from "../env_vars";
import { DirectoryUploader } from "./r2_directory_uploader";
import { FileUploader } from "./r2_file_uploader";
import { S3_CLIENT, initS3Client } from "./s3_client";
import { DeleteObjectsCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { newInternalServerErrorError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { assertReject, assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { mkdir, rm, writeFile } from "fs/promises";

let LOCAL_TEMP_DIR = "temp_dir";
let REMOTE_TEMP_DIR = "remote_temp_dir";

function createDirectoryUploader(
  maxConcurrentUploads: number,
): DirectoryUploader {
  return new DirectoryUploader(
    FileUploader.create(),
    "",
    LOCAL_TEMP_DIR,
    ENV_VARS.r2VideoBucketName,
    REMOTE_TEMP_DIR,
    maxConcurrentUploads,
  );
}

function createFakeFiles(count: number): Array<Promise<void>> {
  return Array.from({ length: count }, (_, i) =>
    writeFile(`${LOCAL_TEMP_DIR}/file_${i}`, "some content"),
  );
}

async function deleteRemoteFiles(count: number): Promise<void> {
  await S3_CLIENT.val.send(
    new DeleteObjectsCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Delete: {
        Objects: Array.from({ length: count }, (_, i) => ({
          Key: `${REMOTE_TEMP_DIR}/file_${i}`,
        })),
      },
    }),
  );
}

TEST_RUNNER.run({
  name: "DirectoryUploaderTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "AllAtOnce",
      execute: async () => {
        // Prepare
        await mkdir(LOCAL_TEMP_DIR);
        await Promise.all(createFakeFiles(10));
        let uploader = createDirectoryUploader(20);

        // Execute
        let bytes = await uploader.upload();

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
      },
      tearDown: async () => {
        await rm(LOCAL_TEMP_DIR, { recursive: true, force: true });
        await deleteRemoteFiles(10);
      },
    },
    {
      name: "SeveralAtATime",
      execute: async () => {
        // Prepare
        await mkdir(LOCAL_TEMP_DIR);
        await Promise.all(createFakeFiles(10));
        let uploader = createDirectoryUploader(2);

        // Execute
        let bytes = await uploader.upload();

        // Verify
        for (let i = 0; i < 10; i++) {
          ``;
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
      },
      tearDown: async () => {
        await rm(LOCAL_TEMP_DIR, { recursive: true, force: true });
        await deleteRemoteFiles(10);
      },
    },
    {
      name: "UploadFileError",
      execute: async () => {
        // Prepare
        await mkdir(LOCAL_TEMP_DIR);
        await createFakeFiles(10);
        let uploaderMock = new (class extends FileUploader {
          private index = 0;
          public constructor() {
            super(undefined, undefined, undefined);
          }
          public async upload(): Promise<void> {
            if (this.index < 5) {
              this.index++;
            } else {
              throw new Error("Fake error");
            }
          }
        })();
        let uploader = new DirectoryUploader(
          uploaderMock,
          "",
          LOCAL_TEMP_DIR,
          ENV_VARS.r2VideoBucketName,
          REMOTE_TEMP_DIR,
          1,
        );

        // Execute
        let error = await assertReject(uploader.upload());

        // Verify
        assertThat(
          error,
          eqHttpError(
            newInternalServerErrorError("Failed to upload the local file temp_dir/file_5"),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await rm(LOCAL_TEMP_DIR, { recursive: true, force: true });
      },
    },
  ],
});
