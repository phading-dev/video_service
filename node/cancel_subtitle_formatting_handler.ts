import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { FormattingState, VideoContainer } from "../db/schema";
import { deleteSubtitleFormattingTaskStatement } from "../db/sql";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { CancelSubtitleFormattingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CancelSubtitleFormattingRequestBody,
  CancelSubtitleFormattingResponse,
} from "@phading/video_service_interface/node/interface";

export class CancelSubtitleFormattingHandler extends CancelSubtitleFormattingHandlerInterface {
  public static create(): CancelSubtitleFormattingHandler {
    return new CancelSubtitleFormattingHandler(CancelFormattingHandler.create);
  }

  private cancelFormattingHandler: CancelFormattingHandler;

  public constructor(
    createCancelFormattingHandler: (
      kind: string,
      getFormattingState: (data: VideoContainer) => FormattingState,
      deleteFormattingTaskStatement: (
        containerId: string,
        gcsFilename: string,
      ) => Statement,
    ) => CancelFormattingHandler,
  ) {
    super();
    this.cancelFormattingHandler = createCancelFormattingHandler(
      "subtitle",
      (data) => data.processing?.subtitle?.formatting,
      deleteSubtitleFormattingTaskStatement,
    );
  }

  public async handle(
    loggingPrefix: string,
    body: CancelSubtitleFormattingRequestBody,
  ): Promise<CancelSubtitleFormattingResponse> {
    return this.cancelFormattingHandler.handle(loggingPrefix, body);
  }
}
