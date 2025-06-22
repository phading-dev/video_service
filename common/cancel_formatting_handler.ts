import { FormattingState, VideoContainer } from "../db/schema";
import {
  getVideoContainer,
  insertGcsKeyDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import {
  CancelFormattingRequestBody,
  CancelFormattingResponse,
} from "./interface";
import { SPANNER_DATABASE } from "./spanner_database";
import { Database } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class CancelFormattingHandler {
  public static create(
    kind: string,
    getFormattingState: (data: VideoContainer) => FormattingState,
    deleteFormattingTaskStatement: (
      containerId: string,
      gcsFilename: string,
    ) => Statement,
  ): CancelFormattingHandler {
    return new CancelFormattingHandler(
      SPANNER_DATABASE,
      () => Date.now(),
      kind,
      getFormattingState,
      deleteFormattingTaskStatement,
    );
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
    private kind: string,
    private getFormattingState: (data: VideoContainer) => FormattingState,
    private deleteFormattingTaskStatement: (
      containerId: string,
      gcsFilename: string,
    ) => Statement,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: CancelFormattingRequestBody,
  ): Promise<CancelFormattingResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerData } = videoContainerRows[0];
      let formatting = this.getFormattingState(videoContainerData);
      if (!formatting) {
        throw newBadRequestError(
          `Video container ${body.containerId} is not in ${this.kind} formatting state.`,
        );
      }
      let gcsFilename = formatting.gcsFilename;
      videoContainerData.processing = undefined;
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        this.deleteFormattingTaskStatement(body.containerId, gcsFilename),
        insertGcsKeyDeletingTaskStatement({
          key: gcsFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
