import { DeleteObjectCommand, ListObjectsV2Command, PutObjectCommand } from "@aws-sdk/client-s3";
import { S3_CLIENT } from "./common/s3_client";
import { createReadStream } from "fs";

async function main() {
  // spawnSync("mkdir", ["-p", `${R2_VIDEO_LOCAL_DIR}/dir`], {
  //   stdio: "inherit",
  // });
  // spawnSync("cp", ["test_data/sub_invalid.txt", `${R2_VIDEO_LOCAL_DIR}/dir/`], {
  //   stdio: "inherit",
  // });
  // spawnSync(
  //   "rclone",
  //   [
  //     "sync",
  //     `${R2_VIDEO_LOCAL_DIR}/dir/`,
  //     `${R2_REMOTE_CONFIG_NAME}:${R2_VIDEO_REMOTE_BUCKET}/dir/`,
  //     "--progress",
  //   ],
  //   {
  //     stdio: "inherit",
  //   },
  // );
  // spawnSync(
  //   "rclone",
  //   ["ls", `${R2_REMOTE_CONFIG_NAME}:${R2_VIDEO_REMOTE_BUCKET}/dir`],
  //   {
  //     stdio: "inherit",
  //   },
  // );

  await S3_CLIENT.send(new PutObjectCommand({
    Bucket: "video-test",
    Key: "sub_invalid.txt",
    Body: createReadStream("test_data/sub_invalid.txt"),
  }));
  console.log(await S3_CLIENT.send(new ListObjectsV2Command({
    Bucket: "video-test",
    Prefix: "sub_invalid.txt",
  })));
  await S3_CLIENT.send(new DeleteObjectCommand({
    Bucket: "video-test",
    Key: "sub_invalid.txt",
  }));
  console.log("xxxxxxxxxxxxxxxxxxxx");
  console.log(await S3_CLIENT.send(new ListObjectsV2Command({
    Bucket: "video-test",
  })));
  await new Promise<void>((resolve) => setTimeout(resolve, 5000));
  console.log("yyyyyyyyyyyyyyyyyyyy");
  console.log(await S3_CLIENT.send(new ListObjectsV2Command({
    Bucket: "video-test",
  })));
}

main();
