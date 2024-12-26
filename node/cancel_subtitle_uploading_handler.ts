import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { ResumableUploadingState, VideoContainer } from "../db/schema";
import { CancelSubtitleUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CancelSubtitleUploadingRequestBody,
  CancelSubtitleUploadingResponse,
} from "@phading/video_service_interface/node/interface";

export class CancelSubtitleUploadingHandler extends CancelSubtitleUploadingHandlerInterface {
  public static create(): CancelSubtitleUploadingHandler {
    return new CancelSubtitleUploadingHandler(
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
        "subtitle",
        (data) => data.processing?.subtitle?.uploading,
      );
  }

  public async handle(
    loggingPrefix: string,
    body: CancelSubtitleUploadingRequestBody,
  ): Promise<CancelSubtitleUploadingResponse> {
    return this.cancelResumableUploadingHandler.handle(loggingPrefix, body);
  }
}
