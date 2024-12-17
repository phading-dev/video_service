import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { FormattingState, VideoContainerData } from "../db/schema";
import { deleteMediaFormattingTaskStatement } from "../db/sql";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { CancelMediaFormattingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CancelMediaFormattingRequestBody,
  CancelMediaFormattingResponse,
} from "@phading/video_service_interface/node/interface";

export class CancelMediaFormattingHandler extends CancelMediaFormattingHandlerInterface {
  public static create(): CancelMediaFormattingHandler {
    return new CancelMediaFormattingHandler(CancelFormattingHandler.create);
  }

  private cancelFormattingHandler: CancelFormattingHandler;

  public constructor(
    createCancelFormattingHandler: (
      kind: string,
      getFormattingState: (data: VideoContainerData) => FormattingState,
      deleteFormattingTaskStatement: (
        containerId: string,
        gcsFilename: string,
      ) => Statement,
    ) => CancelFormattingHandler,
  ) {
    super();
    this.cancelFormattingHandler = createCancelFormattingHandler(
      "media",
      (data) => data.processing?.media?.formatting,
      deleteMediaFormattingTaskStatement,
    );
  }

  public async handle(
    loggingPrefix: string,
    body: CancelMediaFormattingRequestBody,
  ): Promise<CancelMediaFormattingResponse> {
    return this.cancelFormattingHandler.handle(loggingPrefix, body);
  }
}