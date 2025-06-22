import { STORAGE_CLIENT } from "./storage_client";
import { Storage } from "@google-cloud/storage";
import { newInternalServerErrorError } from "@selfage/http_error";
import { readdir } from "fs/promises";

export class GcsDirWritesVerifier {
  public static create(
    localMountedDir: string,
    remoteBucket: string,
    dir: string,
  ): GcsDirWritesVerifier {
    return new GcsDirWritesVerifier(
      setTimeout,
      STORAGE_CLIENT,
      localMountedDir,
      remoteBucket,
      dir,
      GcsDirWritesVerifier.DEFAULT_MAX_VERIFICATIONS,
    );
  }

  private static DEFAULT_MAX_VERIFICATIONS = 100;
  private static POLL_INTERVAL_MS = 500;
  private static POLL_MAX_ATTEMPTS = 10;

  private index = 0;
  private files: Array<string>;

  public constructor(
    private setTimeoutFn: typeof setTimeout,
    private storageClient: Storage,
    private localMountedDir: string,
    private remoteBucket: string,
    private dir: string,
    private maxConcurrentVerifications: number,
  ) {}

  public async verify(): Promise<void> {
    this.files = await readdir(`${this.localMountedDir}/${this.dir}`);
    let workers = Array.from(
      { length: Math.min(this.maxConcurrentVerifications, this.files.length) },
      () => this.verifyOneByOne(),
    );
    await Promise.all(workers);
  }

  private async verifyOneByOne(): Promise<void> {
    while (this.index < this.files.length) {
      let file = this.files[this.index];
      this.index++;
      let attempts = 0;
      while (
        !(await this.storageClient
          .bucket(this.remoteBucket)
          .file(`${this.dir}/${file}`)
          .exists())
      ) {
        attempts++;
        if (attempts > GcsDirWritesVerifier.POLL_MAX_ATTEMPTS) {
          throw newInternalServerErrorError(
            `${this.remoteBucket}/${this.dir}/${file} is still not found after ${GcsDirWritesVerifier.POLL_MAX_ATTEMPTS} attempts.`,
          );
        }
        await new Promise((resolve) =>
          this.setTimeoutFn(resolve, GcsDirWritesVerifier.POLL_INTERVAL_MS),
        );
      }
    }
  }
}
