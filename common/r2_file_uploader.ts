import { S3_CLIENT } from "./s3_client";
import { S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { Ref } from "@selfage/ref";
import { ReadStream } from "fs";

export class FileUploader {
  public static create(): FileUploader {
    return new FileUploader(S3_CLIENT, setTimeout, clearTimeout);
  }

  private static UPLOAD_TIMEOUT_MS = 60000;
  private static UPLOAD_RETRY_LIMIT = 3;

  public constructor(
    private s3ClientRef: Ref<S3Client>,
    private setTimeout: (callback: () => void, ms: number) => NodeJS.Timeout,
    private clearTimeout: (timeoutId: NodeJS.Timeout) => void,
  ) {}

  public async upload(
    bucket: string,
    key: string,
    body: ReadStream | string,
  ): Promise<void> {
    let i = 0;
    while (true) {
      let abortController = new AbortController();
      let timeoutId = this.setTimeout(
        () => abortController.abort(),
        FileUploader.UPLOAD_TIMEOUT_MS,
      );
      try {
        await new Upload({
          client: this.s3ClientRef.val,
          params: {
            Bucket: bucket,
            Key: key,
            Body: body,
          },
          abortController,
        }).done();
        this.clearTimeout(timeoutId);
        break;
      } catch (e) {
        this.clearTimeout(timeoutId);
        if (
          e.code === "ECONNRESET" ||
          e.code === "ETIMEDOUT" ||
          e.code === "ECONNREFUSED"
        ) {
          continue;
        }
        i++;
        if (i >= FileUploader.UPLOAD_RETRY_LIMIT) {
          throw e;
        }
      }
    }
  }
}

export let FILE_UPLOADER = FileUploader.create();
