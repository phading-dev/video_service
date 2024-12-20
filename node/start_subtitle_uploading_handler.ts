import mime = require("mime-types");
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  ACCEPTED_SUBTITLE_ZIP_TYPES,
  MAX_SUBTITLE_ZIP_CONTENT_LENGTH,
} from "@phading/constants/video";
import { StartSubtitleUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  StartSubtitleUploadingRequestBody,
  StartSubtitleUploadingResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";

export class StartSubtitleUploadingHandler extends StartSubtitleUploadingHandlerInterface {
  public static create(): StartSubtitleUploadingHandler {
    return new StartSubtitleUploadingHandler(
      StartResumableUploadingHandler.create(
        "subtitle",
        (data) => data.processing?.subtitle?.uploading,
        (data, uploading) => (data.processing = { subtitle: { uploading } }),
      ),
    );
  }

  public constructor(
    private startResumableUploadingHandler: StartResumableUploadingHandler,
  ) {
    super();
  }

  public handle(
    loggingPrefix: string,
    body: StartSubtitleUploadingRequestBody,
  ): Promise<StartSubtitleUploadingResponse> {
    if (!ACCEPTED_SUBTITLE_ZIP_TYPES.has(body.fileType)) {
      throw newBadRequestError(`File type ${body.fileType} is not accepted.`);
    }
    if (body.contentLength > MAX_SUBTITLE_ZIP_CONTENT_LENGTH) {
      throw newBadRequestError(
        `Content length ${body.contentLength} is too large.`,
      );
    }
    let contentType = mime.contentType(body.fileType) as string;
    return this.startResumableUploadingHandler.handle(loggingPrefix, {
      containerId: body.containerId,
      contentLength: body.contentLength,
      contentType,
    });
  }
}
