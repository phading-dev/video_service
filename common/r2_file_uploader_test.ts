import { ENV_VARS } from "../env";
import { FileUploader } from "./r2_file_uploader";
import { S3_CLIENT, initS3Client } from "./s3_client";
import { DeleteObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { assertReject, assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "FileUploaderTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "Upload",
      execute: async () => {
        // Prepare
        let uploader = new FileUploader(S3_CLIENT, setTimeout, clearTimeout);

        // Execute
        await uploader.upload(
          ENV_VARS.r2VideoBucketName,
          "dir/test_file",
          "some content",
        );

        // Verify
        assertThat(
          await (
            await S3_CLIENT.val.send(
              new GetObjectCommand({
                Bucket: ENV_VARS.r2VideoBucketName,
                Key: "dir/test_file",
              }),
            )
          ).Body.transformToString(),
          eq("some content"),
          "Uploaded content",
        );
      },
      tearDown: async () => {
        await S3_CLIENT.val.send(
          new DeleteObjectCommand({
            Bucket: ENV_VARS.r2VideoBucketName,
            Key: "dir/test_file",
          }),
        );
      },
    },
    {
      name: "UploadTimeoutWithRetries",
      execute: async () => {
        // Prepare
        let uploader = new FileUploader(
          S3_CLIENT,
          (callback, ms) => {
            callback();
            return 1 as any;
          },
          () => {},
        );

        // Execute
        let promise = uploader.upload(
          ENV_VARS.r2VideoBucketName,
          "dir/test_file",
          "some content",
        );
        let error = await assertReject(promise);

        // Verify
        assertThat(error.name, eq("AbortError"), "Error name");
      },
      tearDown: async () => {
        await S3_CLIENT.val.send(
          new DeleteObjectCommand({
            Bucket: ENV_VARS.r2VideoBucketName,
            Key: "dir/test_file",
          }),
        );
      },
    },
  ],
});
