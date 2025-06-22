import { S3Client } from "@aws-sdk/client-s3";
import { Ref } from "@selfage/ref";

export let S3_CLIENT = new Ref<S3Client>();

export function initS3Client(
  cloudflareAccountId: string,
  cloudflareR2AccessKeyId: string,
  cloudflareR2SecretAccessKey: string,
): void {
  S3_CLIENT.val = new S3Client({
    region: "auto",
    endpoint: `https://${cloudflareAccountId}.r2.cloudflarestorage.com`,
    credentials: {
      accessKeyId: cloudflareR2AccessKeyId,
      secretAccessKey: cloudflareR2SecretAccessKey,
    },
  });
}
