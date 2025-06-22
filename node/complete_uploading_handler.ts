import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  STORAGE_RESUMABLE_UPLOAD_CLIENT,
  StorageResumableUploadClient,
} from "../common/storage_resumable_upload_client";
import {
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertSubtitleFormattingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import {
  ACCEPTED_AUDIO_TYPES,
  ACCEPTED_SUBTITLE_ZIP_TYPES,
  ACCEPTED_VIDEO_TYPES,
} from "@phading/constants/video";
import { CompleteUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CompleteUploadingRequestBody,
  CompleteUploadingResponse,
} from "@phading/video_service_interface/node/interface";
import {
  newBadRequestError,
  newConflictError,
  newInternalServerErrorError,
  newNotFoundError,
} from "@selfage/http_error";

export class CompleteUploadingHandler extends CompleteUploadingHandlerInterface {
  public static create(): CompleteUploadingHandler {
    return new CompleteUploadingHandler(
      SPANNER_DATABASE,
      STORAGE_RESUMABLE_UPLOAD_CLIENT,
      () => Date.now(),
    );
  }

  public constructor(
    private database: Database,
    private resumeableUploadClient: StorageResumableUploadClient,
    private getNow: () => number,
  ) {
    super();
  }

  public interfaceFn: () => Promise<void> = () => Promise.resolve();

  public async handle(
    loggingPrefix: string,
    body: CompleteUploadingRequestBody,
  ): Promise<CompleteUploadingResponse> {
    {
      let videoContainerRows = await getVideoContainer(this.database, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let videoContainer = videoContainerRows[0].videoContainerData;
      let uploadingState = videoContainer.processing?.uploading;
      if (!uploadingState) {
        throw newBadRequestError(
          `Video container ${body.containerId} is not in uploading state.`,
        );
      }
      if (body.uploadSessionUrl !== uploadingState.uploadSessionUrl) {
        throw newBadRequestError(
          `Video container ${body.containerId}'s upload session URL is ${uploadingState.uploadSessionUrl} which doesn't match ${body.uploadSessionUrl}.`,
        );
      }
      let { byteOffset } =
        await this.resumeableUploadClient.checkResumableUploadProgress(
          uploadingState.uploadSessionUrl,
          uploadingState.contentLength,
        );
      if (byteOffset !== uploadingState.contentLength) {
        throw newBadRequestError(
          `Video container ${body.containerId} has not finished uploading.`,
        );
      }
    }
    await this.interfaceFn();
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newConflictError(
          `Video container ${body.containerId} is not found anymore.`,
        );
      }
      let { videoContainerData } = videoContainerRows[0];
      let uploadingState = videoContainerData.processing?.uploading;
      if (!uploadingState) {
        throw newConflictError(
          `Video container ${body.containerId} is not in uploading state anymore.`,
        );
      }
      if (body.uploadSessionUrl !== uploadingState.uploadSessionUrl) {
        throw newConflictError(
          `Video container ${body.containerId}'s upload session URL has been changed from ${body.uploadSessionUrl} to ${uploadingState.uploadSessionUrl}.`,
        );
      }
      let gcsFilename = uploadingState.gcsFilename;
      let now = this.getNow();
      let insertFormattingTaskStatement: Statement;
      if (
        ACCEPTED_VIDEO_TYPES.has(uploadingState.fileExt) ||
        ACCEPTED_AUDIO_TYPES.has(uploadingState.fileExt)
      ) {
        videoContainerData.processing = {
          mediaFormatting: {
            gcsFilename,
          },
        };
        insertFormattingTaskStatement = insertMediaFormattingTaskStatement({
          containerId: body.containerId,
          gcsFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        });
      } else if (ACCEPTED_SUBTITLE_ZIP_TYPES.has(uploadingState.fileExt)) {
        videoContainerData.processing = {
          subtitleFormatting: {
            gcsFilename,
          },
        };
        insertFormattingTaskStatement = insertSubtitleFormattingTaskStatement({
          containerId: body.containerId,
          gcsFilename,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        });
      } else {
        throw newInternalServerErrorError(
          `Unhandled file type ${uploadingState.fileExt}.`,
        );
      }
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        insertFormattingTaskStatement,
      ]);
      await transaction.commit();
    });
    return {};
  }
}
