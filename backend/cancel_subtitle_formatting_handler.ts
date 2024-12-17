import { CancelFormattingHandler } from "../common/cancel_formatting_handler";
import { FormattingState, VideoContainerData } from "../db/schema";
import { deleteSubtitleFormattingTaskStatement } from "../db/sql";
import {
  CancelSubtitleFormattingRequestBody,
  CancelSubtitleFormattingResponse,
} from "../interface";
import { Statement } from "@google-cloud/spanner/build/src/transaction";

export class CancelSubtitleFormattingHandler {
  public static create(): CancelSubtitleFormattingHandler {
    return new CancelSubtitleFormattingHandler(CancelFormattingHandler.create);
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
