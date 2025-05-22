import crypto = require("crypto");
import {
  CLOUD_STORAGE_CLIENT,
  CloudStorageClient,
} from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { ResumableUploadingState } from "../db/schema";
import {
  getVideoContainer,
  insertGcsFileStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { Database } from "@google-cloud/spanner";
import {
  ACCEPTED_AUDIO_TYPES,
  ACCEPTED_SUBTITLE_ZIP_TYPES,
  ACCEPTED_VIDEO_TYPES,
  MAX_MEDIA_CONTENT_LENGTH,
  MAX_SUBTITLE_ZIP_CONTENT_LENGTH,
} from "@phading/constants/video";
import { StartUploadingHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  StartUploadingRequestBody,
  StartUploadingResponse,
} from "@phading/video_service_interface/node/interface";
import {
  newBadRequestError,
  newConflictError,
  newNotFoundError,
} from "@selfage/http_error";

export class StartUploadingHandler extends StartUploadingHandlerInterface {
  public static create(): StartUploadingHandler {
    return new StartUploadingHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      () => crypto.randomUUID(),
    );
  }

  public interfereFn: () => Promise<void> = () => Promise.resolve();

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
    private generateUuid: () => string,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: StartUploadingRequestBody,
  ): Promise<StartUploadingResponse> {
    if (
      !ACCEPTED_VIDEO_TYPES.has(body.fileExt) &&
      !ACCEPTED_AUDIO_TYPES.has(body.fileExt) &&
      !ACCEPTED_SUBTITLE_ZIP_TYPES.has(body.fileExt)
    ) {
      throw newBadRequestError(`File type ${body.fileExt} is not accepted.`);
    }
    if (
      (ACCEPTED_VIDEO_TYPES.has(body.fileExt) ||
        ACCEPTED_AUDIO_TYPES.has(body.fileExt)) &&
      body.contentLength > MAX_MEDIA_CONTENT_LENGTH
    ) {
      throw newBadRequestError(
        `Content length ${body.contentLength} is too large.`,
      );
    }
    if (
      ACCEPTED_SUBTITLE_ZIP_TYPES.has(body.fileExt) &&
      body.contentLength > MAX_SUBTITLE_ZIP_CONTENT_LENGTH
    ) {
      throw newBadRequestError(
        `Content length ${body.contentLength} is too large.`,
      );
    }
    let existingUploadingState: ResumableUploadingState;
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
      if (videoContainerData.processing) {
        existingUploadingState = videoContainerData.processing.uploading;
        if (!existingUploadingState) {
          throw newBadRequestError(
            `Video container ${body.containerId} is in other processing state than uploading state.`,
          );
        }
        if (existingUploadingState.contentLength !== body.contentLength) {
          throw newBadRequestError(
            `Video container ${body.containerId} is uploading with content length ${existingUploadingState.contentLength} which doesn't match content length ${body.contentLength} from request.`,
          );
        }
        if (existingUploadingState.fileExt !== body.fileExt) {
          throw newBadRequestError(
            `Video container ${body.containerId} is uploading with file extension ${existingUploadingState.fileExt} which doesn't match file extension ${body.fileExt} from request.`,
          );
        }
        if (existingUploadingState.md5 !== body.md5) {
          throw newBadRequestError(
            `Video container ${body.containerId} is uploading with md5 ${existingUploadingState.md5} which doesn't match md5 ${body.md5} from request.`,
          );
        }
      } else {
        videoContainerData.lastProcessingFailure = undefined;
        let newGcsFilename = this.generateUuid();
        videoContainerData.processing = {
          uploading: {
            gcsFilename: newGcsFilename,
          },
        };
        await transaction.batchUpdate([
          updateVideoContainerStatement({
            videoContainerContainerIdEq: body.containerId,
            setData: videoContainerData,
          }),
          insertGcsFileStatement({ filename: newGcsFilename }),
        ]);
        await transaction.commit();
        existingUploadingState = videoContainerData.processing.uploading;
      }
    });

    let { urlValid, byteOffset } =
      await this.gcsClient.checkResumableUploadProgress(
        existingUploadingState.uploadSessionUrl,
        body.contentLength,
      );
    if (urlValid) {
      return {
        uploadSessionUrl: existingUploadingState.uploadSessionUrl,
        byteOffset,
      };
    }

    let newUploadSessionUrl = await this.gcsClient.createResumableUploadUrl(
      ENV_VARS.gcsVideoBucketName,
      existingUploadingState.gcsFilename,
      body.contentLength,
    );
    await this.interfereFn();
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
      if (
        uploadingState.uploadSessionUrl !==
        existingUploadingState.uploadSessionUrl
      ) {
        throw newConflictError(
          `Video container ${body.containerId}'s upload session URL has been changed from ${existingUploadingState.uploadSessionUrl} to ${uploadingState.uploadSessionUrl}.`,
        );
      }
      if (uploadingState.gcsFilename !== existingUploadingState.gcsFilename) {
        throw newConflictError(
          `Video container ${body.containerId}'s GCS filename has been changed from ${existingUploadingState.gcsFilename} to ${uploadingState.gcsFilename}.`,
        );
      }
      uploadingState.uploadSessionUrl = newUploadSessionUrl;
      uploadingState.contentLength = body.contentLength;
      uploadingState.fileExt = body.fileExt;
      uploadingState.md5 = body.md5;
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
      ]);
      await transaction.commit();
    });
    return {
      uploadSessionUrl: newUploadSessionUrl,
      byteOffset,
    };
  }
}
