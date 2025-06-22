import { S3_CLIENT } from "./s3_client";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
// import { Upload } from "@aws-sdk/lib-storage";
import { Ref } from "@selfage/ref";
import { createReadStream } from "fs";
import { stat } from "fs/promises";

export class FileUploader {
  public static create(): FileUploader {
    return new FileUploader(S3_CLIENT, setTimeout, clearTimeout);
  }

  private static UPLOAD_TIMEOUT_MS = 5 * 60 * 1000;
  private static UPLOAD_RETRY_LIMIT = 3;

  public constructor(
    private s3ClientRef: Ref<S3Client>,
    private setTimeout: (callback: () => void, ms: number) => NodeJS.Timeout,
    private clearTimeout: (timeoutId: NodeJS.Timeout) => void,
  ) {}

  public async upload(
    loggingPrefix: string,
    bucket: string,
    key: string,
    localFilePath: string,
    // body: ReadStream | string,
  ): Promise<void> {
    let fileStat = await stat(localFilePath);
    let i = 0;
    while (true) {
      let abortController = new AbortController();
      let upload = this.s3ClientRef.val.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: createReadStream(localFilePath),
          ContentLength: fileStat.size,
        }),
        {
          abortSignal: abortController.signal,
        }
      );
      // let upload = new Upload({
      //   client: this.s3ClientRef.val,
      //   params: {
      //     Bucket: bucket,
      //     Key: key,
      //     Body: body,
      //   },
      // });
      let timeoutId = this.setTimeout(() => {
        console.error(`${loggingPrefix} Upload to ${bucket}/${key} timed out.`);
        abortController.abort();
      }, FileUploader.UPLOAD_TIMEOUT_MS);
      try {
        await upload;
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
