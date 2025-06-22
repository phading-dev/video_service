import {
  RCLONE_CONFIGURE_FILE,
  RCLONE_GCS_REMOTE_NAME,
  RCLONE_R2_REMOTE_NAME,
} from "./common/constants";
import { writeFile } from "fs/promises";

export async function configureRclone(
  cloudflareAccountId: string,
  cloudflareR2AccessKeyId: string,
  cloudflareR2SecretAccessKey: string,
): Promise<void> {
  await writeFile(
    RCLONE_CONFIGURE_FILE,
    `
[${RCLONE_GCS_REMOTE_NAME}]
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
