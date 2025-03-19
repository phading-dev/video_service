import { CompleteResumableUploadingHandler } from "../common/complete_resumable_uploading_handler";
import {
  FormattingState,
  ResumableUploadingState,
  VideoContainer,
} from "../db/schema";
import { insertSubtitleFormattingTaskStatement } from "../db/sql";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { CompleteSubtitleUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CompleteSubtitleUploadingRequestBody,
  CompleteSubtitleUploadingResponse,
} from "@phading/video_service_interface/node/interface";

export class CompleteSubtitleUploadingHandler extends CompleteSubtitleUploadingHandlerInterface {
  public static create(): CompleteSubtitleUploadingHandler {
    return new CompleteSubtitleUploadingHandler(
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
      insertFormattingTaskStatement: (args: {
        containerId: string;
        gcsFilename: string;
        retryCount: number;
        executionTimeMs: number;
        createdTimeMs: number;
      }) => Statement,
    ) => CompleteResumableUploadingHandler,
  ) {
    super();
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
