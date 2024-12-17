import mime = require("mime-types");
import { StartResumableUploadingHandler } from "../common/start_resumable_uploading_handler";
import { ACCEPTED_MEDIA_TYPES } from "@phading/constants/video";
import { StartMediaUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  StartMediaUploadingRequestBody,
  StartMediaUploadingResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError } from "@selfage/http_error";

export class StartMediaUploadingHandler extends StartMediaUploadingHandlerInterface {
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
  ) {
    super();
  }

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
