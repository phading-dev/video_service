import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertGcsUploadFileDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { CancelUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CancelUploadingRequestBody,
  CancelUploadingResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class CancelUploadingHandler extends CancelUploadingHandlerInterface {
  public static create(): CancelUploadingHandler {
    return new CancelUploadingHandler(SPANNER_DATABASE, () => Date.now());
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: CancelUploadingRequestBody,
  ): Promise<CancelUploadingResponse> {
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
      let uploadingState = videoContainerData.processing?.uploading;
      if (!uploadingState) {
        throw newBadRequestError(
          `Video container ${body.containerId} is not in uploading state.`,
        );
      }
      let gcsFilename = uploadingState.gcsFilename;
      videoContainerData.processing = undefined;
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        insertGcsUploadFileDeletingTaskStatement({
          filename: gcsFilename,
          uploadSessionUrl: uploadingState.uploadSessionUrl,
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
