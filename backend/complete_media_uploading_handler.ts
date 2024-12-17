import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  FormattingState,
  ResumableUploadingState,
  VideoContainerData,
} from "../db/schema";
import { insertMediaFormattingTaskStatement } from "../db/sql";
import {
  CompleteMediaUploadingRequestBody,
  CompleteMediaUploadingResponse,
} from "../interface";
import { Statement } from "@google-cloud/spanner/build/src/transaction";

export class CompleteMediaUploadingHandler {
  public static create(): CompleteMediaUploadingHandler {
    return new CompleteMediaUploadingHandler(
      CompleteResumableUploadingHandler.create,
    );
  }

  private completeResumableUploadingHandler: CompleteResumableUploadingHandler;

  public constructor(
    createCompleteResumableUploadingHandler: (
      kind: string,
      getUploadingState: (data: VideoContainerData) => ResumableUploadingState,
      saveFormattingState: (
        data: VideoContainerData,
        state: FormattingState,
      ) => void,
      insertFormattingTaskStatement: (
        containerId: string,
        gcsFilename: string,
        executionTimestamp: number,
        createdTimestamp: number,
      ) => Statement,
    ) => CompleteResumableUploadingHandler,
  ) {
    this.completeResumableUploadingHandler =
      createCompleteResumableUploadingHandler(
        "media",
        (data) => data.processing?.media?.uploading,
        (data, formatting) => (data.processing = { media: { formatting } }),
        insertMediaFormattingTaskStatement,
      );
  }

  public async handle(
    loggingPrefix: string,
    body: CompleteMediaUploadingRequestBody,
  ): Promise<CompleteMediaUploadingResponse> {
    return this.completeResumableUploadingHandler.handle(loggingPrefix, body);
  }
}
