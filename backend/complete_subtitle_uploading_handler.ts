import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  FormattingState,
  ResumableUploadingState,
  VideoContainerData,
} from "../db/schema";
import { insertSubtitleFormattingTaskStatement } from "../db/sql";
import {
  CompleteSubtitleUploadingRequestBody,
  CompleteSubtitleUploadingResponse,
} from "../interface";
import { Statement } from "@google-cloud/spanner/build/src/transaction";

export class CompleteSubtitleUploadingHandler {
  public static create(): CompleteSubtitleUploadingHandler {
    return new CompleteSubtitleUploadingHandler(
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
        "subtitle",
        (data) => data.processing?.subtitle?.uploading,
        (data, formatting) => (data.processing = { subtitle: { formatting } }),
        insertSubtitleFormattingTaskStatement,
      );
  }

  public async handle(
    loggingPrefix: string,
    body: CompleteSubtitleUploadingRequestBody,
  ): Promise<CompleteSubtitleUploadingResponse> {
    return this.completeResumableUploadingHandler.handle(loggingPrefix, body);
  }
}
