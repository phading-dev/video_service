import getStream from "get-stream";
import {
  RCLONE_CONFIGURE_FILE,
  RCLONE_GCS_REMOTE_NAME,
  RCLONE_R2_REMOTE_NAME,
} from "./common/constants";
import { STORAGE_CLIENT } from "./common/storage_client";
import { ENV_VARS } from "./env_vars";
import { writeFile } from "fs/promises";

export async function configureRclone(): Promise<void> {
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
  await writeFile(
    RCLONE_CONFIGURE_FILE,
    `[${RCLONE_GCS_REMOTE_NAME}]
type = google cloud storage

[${RCLONE_R2_REMOTE_NAME}]
type = s3
provider = Cloudflare
access_key_id = ${cloudflareR2AccessKeyId}
secret_access_key = ${cloudflareR2SecretAccessKey}
endpoint = https://${cloudflareAccountId}.r2.cloudflarestorage.com
`,
  );
}
