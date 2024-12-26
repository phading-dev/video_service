import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  FormattingState,
  ResumableUploadingState,
  VideoContainer,
} from "../db/schema";
import { insertMediaFormattingTaskStatement } from "../db/sql";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { CompleteMediaUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CompleteMediaUploadingRequestBody,
  CompleteMediaUploadingResponse,
} from "@phading/video_service_interface/node/interface";

export class CompleteMediaUploadingHandler extends CompleteMediaUploadingHandlerInterface {
  public static create(): CompleteMediaUploadingHandler {
    return new CompleteMediaUploadingHandler(
      CompleteResumableUploadingHandler.create,
    );
  }

  private completeResumableUploadingHandler: CompleteResumableUploadingHandler;

  public constructor(
    createCompleteResumableUploadingHandler: (
      kind: string,
      getUploadingState: (data: VideoContainer) => ResumableUploadingState,
      saveFormattingState: (
        data: VideoContainer,
        state: FormattingState,
      ) => void,
      insertFormattingTaskStatement: (
        containerId: string,
        gcsFilename: string,
        executionTimeMs: number,
        createdTimeMs: number,
      ) => Statement,
    ) => CompleteResumableUploadingHandler,
  ) {
    super();
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
