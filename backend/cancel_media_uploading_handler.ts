import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { ResumableUploadingState, VideoContainerData } from "../db/schema";
import {
  CancelMediaUploadingRequestBody,
  CancelMediaUploadingResponse,
} from "../interface";

export class CancelMediaUploadingHandler {
  public static create(): CancelMediaUploadingHandler {
    return new CancelMediaUploadingHandler(
      CancelResumableUploadingHandler.create,
    );
  }

  private cancelResumableUploadingHandler: CancelResumableUploadingHandler;

  public constructor(
    createCancelResumableUploadingHandler: (
      kind: string,
      getUploadingState: (data: VideoContainerData) => ResumableUploadingState,
    ) => CancelResumableUploadingHandler,
  ) {
    this.cancelResumableUploadingHandler =
      createCancelResumableUploadingHandler(
        "media",
        (data) => data.processing?.media?.uploading,
      );
  }

  public async handle(
    loggingPrefix: string,
    body: CancelMediaUploadingRequestBody,
  ): Promise<CancelMediaUploadingResponse> {
    return this.cancelResumableUploadingHandler.handle(loggingPrefix, body);
  }
}
