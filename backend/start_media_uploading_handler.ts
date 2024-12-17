import mime = require("mime-types");
import { ACCEPTED_MEDIA_TYPES } from "../common/params";
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import {
  StartMediaUploadingRequestBody,
  StartMediaUploadingResponse,
} from "../interface";
import { newBadRequestError } from "@selfage/http_error";

export class StartMediaUploadingHandler {
  public static create(): StartMediaUploadingHandler {
    return new StartMediaUploadingHandler(
      StartResumableUploadingHandler.create(
        "media",
        (data) => data.processing?.media?.uploading,
        (data, uploading) => (data.processing = { media: { uploading } }),
      ),
    );
  }

  public constructor(
    private startResumableUploadingHandler: StartResumableUploadingHandler,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: StartMediaUploadingRequestBody,
  ): Promise<StartMediaUploadingResponse> {
    if (!ACCEPTED_MEDIA_TYPES.has(body.fileType)) {
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
