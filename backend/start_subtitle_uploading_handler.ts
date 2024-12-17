import mime = require("mime-types");
import { ACCEPTED_SUBTITLE_ZIP_TYPES } from "../common/params";
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  StartSubtitleUploadingRequestBody,
  StartSubtitleUploadingResponse,
} from "../interface";
import { newBadRequestError } from "@selfage/http_error";

export class StartSubtitleUploadingHandler {
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
  ) {}

  public handle(
    loggingPrefix: string,
    body: StartSubtitleUploadingRequestBody,
  ): Promise<StartSubtitleUploadingResponse> {
    if (!ACCEPTED_SUBTITLE_ZIP_TYPES.has(body.fileType)) {
      throw newBadRequestError(`File type ${body.fileType} is not accepted.`);
    }
    let contentType = mime.contentType(body.fileType) as string;
    return this.startResumableUploadingHandler.handle(loggingPrefix, {
      containerId: body.containerId,
      contentLength: body.contentLength,
      contentType,
    });
  }
}
