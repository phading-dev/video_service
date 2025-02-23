import { SPANNER_DATABASE } from "../common/spanner_database";
import { ResumableUploadingState, VideoContainer } from "../db/schema";
import {
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import {
  CancelResumableUploadingRequestBody,
  CancelResumableUploadingResponse,
} from "./interface";
import { Database } from "@google-cloud/spanner";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class CancelResumableUploadingHandler {
  public static create(
    kind: string,
    getUploadingState: (data: VideoContainer) => ResumableUploadingState,
  ): CancelResumableUploadingHandler {
    return new CancelResumableUploadingHandler(
      SPANNER_DATABASE,
      () => Date.now(),
      kind,
      getUploadingState,
    );
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
    private kind: string,
    private getUploadingState: (
      data: VideoContainer,
    ) => ResumableUploadingState,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: CancelResumableUploadingRequestBody,
  ): Promise<CancelResumableUploadingResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(
        transaction,
        body.containerId,
      );
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerData } = videoContainerRows[0];
      let uploadingState = this.getUploadingState(videoContainerData);
      if (!uploadingState) {
        throw newBadRequestError(
          `Video container ${body.containerId} is not in ${this.kind} uploading state.`,
        );
      }
      let gcsFilename = uploadingState.gcsFilename;
      videoContainerData.processing = undefined;
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainerData),
        insertGcsFileDeletingTaskStatement(
          gcsFilename,
          uploadingState.uploadSessionUrl,
          0,
          now,
          now,
        ),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
