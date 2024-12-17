import { FILE_UPLOADER, FileUploader } from "./r2_file_uploader";
import { newInternalServerErrorError } from "@selfage/http_error";
import { FSWatcher, createReadStream, watch } from "fs";
import { readdir, rm, stat } from "fs/promises";

export class DirectoryStreamUploader {
  public static create(
    loggingPrefix: string,
    localDir: string,
    remoteBucket: string,
    remoteDir: string,
  ): DirectoryStreamUploader {
    return new DirectoryStreamUploader(
      FILE_UPLOADER,
      loggingPrefix,
      localDir,
      remoteBucket,
      remoteDir,
    );
  }

  public interfereSendFn: () => void = () => {};
  private deleting = new Set<string>();
  private uploading = new Map<string, Promise<void>>();
  private filesWithError = new Array<string>();
  private totalBytes = 0;
  private pending: string;
  private watcher: FSWatcher;

  public constructor(
    private fileUploader: FileUploader,
    private loggingPrefix: string,
    private localDir: string,
    private remoteBucket: string,
    private remoteDir: string,
  ) {}

  public start(): this {
    this.watcher = watch(this.localDir, (event, filename) => {
      if (event !== "rename") {
        return;
      }
      // "rename" event is fired for either added or deleted file.
      if (this.deleting.has(filename)) {
        // Probably deleted.
        this.deleting.delete(filename);
        this.uploading.delete(filename);
      } else {
        // Probably added.
        // Uploads the last added file.
        if (this.pending && !this.uploading.has(this.pending)) {
          this.uploading.set(this.pending, this.uploadAndDelete(this.pending));
        }
        this.pending = filename;
      }
    });
    return this;
  }

  private async uploadAndDelete(filename: string): Promise<void> {
    let info = await stat(`${this.localDir}/${filename}`);
    this.totalBytes += info.size;
    try {
      await this.fileUploader.upload(
        this.remoteBucket,
        `${this.remoteDir}/${filename}`,
        createReadStream(`${this.localDir}/${filename}`),
      );
    } catch (e) {
      console.log(
        `${this.loggingPrefix} Failed to upload ${this.localDir}/${filename}.`,
        e,
      );
      this.filesWithError.push(`${this.localDir}/${filename}`);
    }
    this.deleting.add(filename);
    await rm(`${this.localDir}/${filename}`, { force: true });
  }

  public async flush(): Promise<number> {
    this.watcher.close();
    let files = await readdir(this.localDir);
    await Promise.all(
      files.map((filename) => {
        if (this.uploading.has(filename)) {
          return this.uploading.get(filename);
        } else {
          return this.uploadAndDelete(filename);
        }
      }),
    );
    await rm(this.localDir, { recursive: true, force: true });
    if (this.filesWithError.length > 0) {
      throw newInternalServerErrorError(
        `Failed to upload the following files: ${this.filesWithError.join(", ")}`,
      );
    }
    console.log(
      `${this.loggingPrefix} Flushed ${this.localDir} with ${this.totalBytes} bytes.`,
    );
    return this.totalBytes;
  }
}
