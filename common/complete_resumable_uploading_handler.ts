import { CLOUD_STORAGE_CLIENT, CloudStorageClient } from "../common/cloud_storage_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  FormattingState,
  ResumableUploadingState,
  VideoContainerData,
} from "../db/schema";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import {
  CompleteResumableUploadingRequestBody,
  CompleteResumableUploadingResponse,
} from "./interface";
import { Database } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import {
  newBadRequestError,
  newConflictError,
  newNotFoundError,
} from "@selfage/http_error";

export class CompleteResumableUploadingHandler {
  public static create(
    kind: string,
    getUploadingState: (data: VideoContainerData) => ResumableUploadingState,
    saveFormattingState: (
      data: VideoContainerData,
      state: FormattingState,
    ) => void,
    insertFormattingTaskStatement: (
      containerId: string,
      gcsFilename: string,
      executionTimestamp: number,
      createdTimestamp: number,
    ) => Statement,
  ): CompleteResumableUploadingHandler {
    return new CompleteResumableUploadingHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      () => Date.now(),
      kind,
      getUploadingState,
      saveFormattingState,
      insertFormattingTaskStatement,
    );
  }

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
    private getNow: () => number,
    private kind: string,
    private getUploadingState: (
      data: VideoContainerData,
    ) => ResumableUploadingState,
    private saveFormattingState: (
      data: VideoContainerData,
      state: FormattingState,
    ) => void,
    private insertFormattingTaskStatement: (
      containerId: string,
      gcsFilename: string,
      executionTimestamp: number,
      createdTimestamp: number,
    ) => Statement,
  ) {}

  public interfaceFn: () => Promise<void> = () => Promise.resolve();

  public async handle(
    loggingPrefix: string,
    body: CompleteResumableUploadingRequestBody,
  ): Promise<CompleteResumableUploadingResponse> {
    {
      let videoContainerRows = await getVideoContainer(
        this.database,
        body.containerId,
      );
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let videoContainer = videoContainerRows[0].videoContainerData;
      let uploadingState = this.getUploadingState(videoContainer);
      if (!uploadingState) {
        throw newBadRequestError(
          `Video container ${body.containerId} is not in ${this.kind} uploading state.`,
        );
      }
      if (body.uploadSessionUrl !== uploadingState.uploadSessionUrl) {
        throw newBadRequestError(
          `Upload session url for ${this.kind} of video container ${body.containerId} is ${uploadingState.uploadSessionUrl} which doesn't match ${body.uploadSessionUrl}.`,
        );
      }
      let { byteOffset } = await this.gcsClient.checkResumableUploadProgress(
        uploadingState.uploadSessionUrl,
        uploadingState.contentLength,
      );
      if (byteOffset !== uploadingState.contentLength) {
        throw newBadRequestError(
          `Video container ${body.containerId} has not finished uploading ${this.kind}.`,
        );
      }
    }
    await this.interfaceFn();
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
      let videoContainer = videoContainerRows[0].videoContainerData;
      let uploadingState = this.getUploadingState(videoContainer);
      if (!uploadingState) {
        throw newConflictError(
          `Video container ${body.containerId} is not in ${this.kind} uploading state anymore.`,
        );
      }
      if (body.uploadSessionUrl !== uploadingState.uploadSessionUrl) {
        throw newConflictError(
          `Upload sesison url for ${this.kind} of video container ${body.containerId} has been changed from ${body.uploadSessionUrl} to ${uploadingState.uploadSessionUrl}.`,
        );
      }
      let gcsFilename = uploadingState.gcsFilename;
      this.saveFormattingState(videoContainer, {
        gcsFilename,
      });
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(body.containerId, videoContainer),
        this.insertFormattingTaskStatement(
          body.containerId,
          gcsFilename,
          now,
          now,
        ),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
