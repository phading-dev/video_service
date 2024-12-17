import { FileUploader } from "./r2_file_uploader";
import { ReadStream } from "fs";

export class FileUploaderMock extends FileUploader {
  public error: Error;
  public constructor() {
    super(undefined, undefined, undefined);
  }
  public async upload(key: string, body: ReadStream | string): Promise<void> {
    if (this.error) {
      throw this.error;
    }
    return;
  }
}
