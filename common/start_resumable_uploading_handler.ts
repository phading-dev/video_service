import crypto = require("crypto");
import {
  CLOUD_STORAGE_CLIENT,
  CloudStorageClient,
} from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { ResumableUploadingState, VideoContainer } from "../db/schema";
import {
  getVideoContainer,
  insertGcsFileStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env";
import {
  StartResumableUploadingRequestBody,
  StartResumableUploadingResponse,
} from "./interface";
import { Database } from "@google-cloud/spanner";
import {
  newBadRequestError,
  newConflictError,
  newNotFoundError,
} from "@selfage/http_error";

export class StartResumableUploadingHandler {
  public static create(
    kind: string,
    getUploadingState: (data: VideoContainer) => ResumableUploadingState,
    saveUploadingState: (
      data: VideoContainer,
      state: ResumableUploadingState,
    ) => void,
  ): StartResumableUploadingHandler {
    return new StartResumableUploadingHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      () => crypto.randomUUID(),
      kind,
      getUploadingState,
      saveUploadingState,
    );
  }

  public interfereFn: () => Promise<void> = () => Promise.resolve();

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
    private generateUuid: () => string,
    private kind: string,
    private getUploadingState: (
      data: VideoContainer,
    ) => ResumableUploadingState,
    private saveUploadingState: (
      data: VideoContainer,
      state: ResumableUploadingState,
    ) => void,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: StartResumableUploadingRequestBody,
  ): Promise<StartResumableUploadingResponse> {
    let existingUploadingState: ResumableUploadingState;
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
      if (videoContainerData.processing) {
        existingUploadingState = this.getUploadingState(videoContainerData);
        if (!existingUploadingState) {
          throw newBadRequestError(
            `Video container ${body.containerId} is in other processing state than uploading state for ${this.kind}.`,
          );
        }
        if (existingUploadingState.contentLength !== body.contentLength) {
          throw newBadRequestError(
            `Content length for ${this.kind} of video container ${body.containerId} is ${existingUploadingState.contentLength} which doesn't match content length ${body.contentLength} from request.`,
          );
        }
        if (existingUploadingState.contentType !== body.contentType) {
          throw newBadRequestError(
            `Content type for ${this.kind} of video container ${body.containerId} is ${existingUploadingState.contentType} which doesn't match content type ${body.contentType} from request.`,
          );
        }
      } else {
        videoContainerData.lastProcessingFailures = [];
        let newGcsFilename = this.generateUuid();
        this.saveUploadingState(videoContainerData, {
          gcsFilename: newGcsFilename,
        });
        await transaction.batchUpdate([
          updateVideoContainerStatement(videoContainerData),
          insertGcsFileStatement(newGcsFilename),
        ]);
        await transaction.commit();
        existingUploadingState = this.getUploadingState(videoContainerData);
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
      body.contentType,
      body.contentLength,
    );
    await this.interfereFn();
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(
        transaction,
        body.containerId,
      );
      if (videoContainerRows.length === 0) {
        throw newConflictError(
          `Video container ${body.containerId} is not found anymore.`,
        );
      }
      let { videoContainerData } = videoContainerRows[0];
      let uploadingState = this.getUploadingState(videoContainerData);
      if (!uploadingState) {
        throw newConflictError(
          `Video container ${body.containerId} is not in uploading state for ${this.kind} anymore.`,
        );
      }
      if (
        uploadingState.uploadSessionUrl !==
        existingUploadingState.uploadSessionUrl
      ) {
        throw newConflictError(
          `Upload session url for ${this.kind} of video container ${body.containerId} has been changed from ${existingUploadingState.uploadSessionUrl} to ${uploadingState.uploadSessionUrl}.`,
        );
      }
      if (uploadingState.gcsFilename !== existingUploadingState.gcsFilename) {
        throw newConflictError(
          `GCS filename for ${this.kind} of video container ${body.containerId} has been changed from ${existingUploadingState.gcsFilename} to ${uploadingState.gcsFilename}.`,
        );
      }
      uploadingState.uploadSessionUrl = newUploadSessionUrl;
      uploadingState.contentLength = body.contentLength;
      uploadingState.contentType = body.contentType;
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainerData),
      ]);
      await transaction.commit();
    });
    return {
      uploadSessionUrl: newUploadSessionUrl,
      byteOffset,
    };
  }
}
