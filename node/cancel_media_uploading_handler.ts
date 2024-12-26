import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { ResumableUploadingState, VideoContainer } from "../db/schema";
import { CancelMediaUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CancelMediaUploadingRequestBody,
  CancelMediaUploadingResponse,
} from "@phading/video_service_interface/node/interface";

export class CancelMediaUploadingHandler extends CancelMediaUploadingHandlerInterface {
  public static create(): CancelMediaUploadingHandler {
    return new CancelMediaUploadingHandler(
      CancelResumableUploadingHandler.create,
    );
  }

  private cancelResumableUploadingHandler: CancelResumableUploadingHandler;

  public constructor(
    createCancelResumableUploadingHandler: (
      kind: string,
      getUploadingState: (data: VideoContainer) => ResumableUploadingState,
    ) => CancelResumableUploadingHandler,
  ) {
    super();
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
