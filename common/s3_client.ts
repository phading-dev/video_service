import getStream from "get-stream";
import { ENV_VARS } from "../env";
import { STORAGE_CLIENT } from "./storage_client";
import { S3Client } from "@aws-sdk/client-s3";
import { Ref } from "@selfage/ref";

export let S3_CLIENT = new Ref<S3Client>();

export async function initS3Client(): Promise<void> {
  let [
    cloudflareAccountId,
    cloudflareR2AccessKeyId,
    cloudflareR2SecretAccessKey,
  ] = await Promise.all([
    getStream(
      STORAGE_CLIENT.bucket(ENV_VARS.gcsSecretBucketName)
        .file(ENV_VARS.cloudflareAccountIdFile)
        .createReadStream(),
    ),
    getStream(
      STORAGE_CLIENT.bucket(ENV_VARS.gcsSecretBucketName)
        .file(ENV_VARS.cloudflareR2AccessKeyIdFile)
        .createReadStream(),
    ),
    getStream(
      STORAGE_CLIENT.bucket(ENV_VARS.gcsSecretBucketName)
        .file(ENV_VARS.cloudflareR2SecretAccessKeyFile)
        .createReadStream(),
    ),
  ]);
  S3_CLIENT.val = new S3Client({
    region: "auto",
    endpoint: `https://${cloudflareAccountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: cloudflareR2AccessKeyId,
      secretAccessKey: cloudflareR2SecretAccessKey,
    },
  });
}
