import { R2_VIDEO_REMOTE_BUCKET } from "./env_vars";
import { FileUploader } from "./r2_file_uploader";
import { S3_CLIENT } from "./s3_client";
import { DeleteObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { assertReject, assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

TEST_RUNNER.run({
  name: "FileUploaderTest",
  cases: [
    {
      name: "Upload",
      execute: async () => {
        // Prepare
        let uploader = new FileUploader(S3_CLIENT, setTimeout, clearTimeout);

        // Execute
        await uploader.upload(
          R2_VIDEO_REMOTE_BUCKET,
          "dir/test_file",
          "some content",
        );

        // Verify
        assertThat(
          await (
            await S3_CLIENT.send(
              new GetObjectCommand({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
                Key: "dir/test_file",
              }),
            )
          ).Body.transformToString(),
          eq("some content"),
          "Uploaded content",
        );
      },
      tearDown: async () => {
        await S3_CLIENT.send(
          new DeleteObjectCommand({
            Bucket: R2_VIDEO_REMOTE_BUCKET,
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
          R2_VIDEO_REMOTE_BUCKET,
          "dir/test_file",
          createReadStream("test_data/video_only.mp4"),
        );
        let error = await assertReject(promise);

        // Verify
        assertThat(error.name, eq("AbortError"), "Error name");
      },
      tearDown: async () => {
        await S3_CLIENT.send(
          new DeleteObjectCommand({
            Bucket: R2_VIDEO_REMOTE_BUCKET,
            Key: "dir/test_file",
          }),
        );
      },
    },
  ],
});
