import { FILE_UPLOADER, FileUploader } from "./r2_file_uploader";
import { newInternalServerErrorError } from "@selfage/http_error";
import { readdir, stat } from "fs/promises";

export class DirectoryUploader {
  public static create(
    loggingPrefix: string,
    localDir: string,
    remoteBucket: string,
    remoteDir: string,
  ): DirectoryUploader {
    return new DirectoryUploader(
      FILE_UPLOADER,
      loggingPrefix,
      localDir,
      remoteBucket,
      remoteDir,
      DirectoryUploader.DEFAULT_MAX_CONCURRENT_UPLOADS,
    );
  }

  private static DEFAULT_MAX_CONCURRENT_UPLOADS = 1;

  private totalBytes = 0;
  private index = 0;
  private files: Array<string>;

  public constructor(
    private fileUploader: FileUploader,
    private loggingPrefix: string,
    private localDir: string,
    private remoteBucket: string,
    private remoteDir: string,
    private maxConcurrentUploads: number,
  ) {}

  public async upload(): Promise<number> {
    this.files = await readdir(this.localDir);
    console.log(
      `${this.loggingPrefix} Uploading ${this.files.length} files from ${this.localDir} to ${this.remoteBucket}/${this.remoteDir}...`,
    );
    let workers = Array.from(
      { length: Math.min(this.maxConcurrentUploads, this.files.length) },
      () => this.uploadOneByOne(),
    );
    await Promise.all(workers);
    console.log(
      `${this.loggingPrefix} Uploaded ${this.localDir} with ${this.totalBytes} bytes.`,
    );
    return this.totalBytes;
  }

  private async uploadOneByOne(): Promise<void> {
    while (this.index < this.files.length) {
      let file = this.files[this.index];
      this.index++;
      let size = await this.uploadAndGetSize(file);
      this.totalBytes += size;
    }
  }

  private async uploadAndGetSize(filename: string): Promise<number> {
    let info = await stat(`${this.localDir}/${filename}`);
    try {
      await this.fileUploader.upload(
        this.loggingPrefix,
        this.remoteBucket,
        `${this.remoteDir}/${filename}`,
        // createReadStream(`${this.localDir}/${filename}`),
        `${this.localDir}/${filename}`,
      );
    } catch (e) {
      throw newInternalServerErrorError(
        `Failed to upload the local file ${this.localDir}/${filename} to ${this.remoteBucket}/${this.remoteDir}/${filename}. Error: ${e.stack}`,
      );
    }
    return info.size;
  }
}
