import { CancelResumableUploadingHandler } from "../common/cancel_resumable_uploading_handler";
import { ResumableUploadingState, VideoContainerData } from "../db/schema";
import {
  CancelSubtitleUploadingRequestBody,
  CancelSubtitleUploadingResponse,
} from "../interface";

export class CancelSubtitleUploadingHandler {
  public static create(): CancelSubtitleUploadingHandler {
    return new CancelSubtitleUploadingHandler(
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
