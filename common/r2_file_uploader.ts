import { S3_CLIENT } from "./s3_client";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ReadStream } from "fs";

export class FileUploader {
  public static create(): FileUploader {
    return new FileUploader(S3_CLIENT, setTimeout, clearTimeout);
  }

  private static UPLOAD_TIMEOUT_MS = 60000;
  private static UPLOAD_RETRY_LIMIT = 3;

  public constructor(
    private s3Client: S3Client,
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
        await this.s3Client.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
          }),
          {
            abortSignal: abortController.signal,
          },
        );
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
