import { CLOUD_STORAGE_CLIENT, CloudStorageClient } from "../common/gcs_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoTrack,
  insertVideoFormattingTaskStatement,
  updateVideoTrackStatement,
} from "../db/sql";
import {
  CompleteVideoUploadRequestBody,
  CompleteVideoUploadResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import {
  newBadRequestError,
  newConflictError,
  newNotFoundError,
} from "@selfage/http_error";

export class CompleteVideoUploadHandler {
  public static create(): CompleteVideoUploadHandler {
    return new CompleteVideoUploadHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      () => Date.now(),
    );
  }

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
    private getNow: () => number,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: CompleteVideoUploadRequestBody,
  ): Promise<CompleteVideoUploadResponse> {
    let rows = await getVideoTrack(
      this.database,
      body.containerId,
      body.videoId,
    );
    if (rows.length === 0) {
      throw newNotFoundError(
        `Container ${body.containerId} video ${body.videoId} is not found.`,
      );
    }
    let row = rows[0];
    if (!row.videoTrackData.uploading) {
      throw newBadRequestError(
        `Container ${body.containerId} video ${body.videoId} is not in uploading state.`,
      );
    }
    let originalUploadSessionUrl =
      row.videoTrackData.uploading.uploadSessionUrl;
    let { byteOffset } = await this.gcsClient.checkResumableUploadProgress(
      row.videoTrackData.uploading.totalBytes,
      originalUploadSessionUrl,
    );
    if (byteOffset !== row.videoTrackData.uploading.totalBytes) {
      throw newBadRequestError(
        `Container ${body.containerId} video ${body.videoId} has not finished uploading.`,
      );
    }
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getVideoTrack(
        transaction,
        body.containerId,
        body.videoId,
      );
      if (rows.length === 0) {
        throw newConflictError(
          `Container ${body.containerId} video ${body.videoId} is not found anymore.`,
        );
      }
      let row = rows[0];
      if (!row.videoTrackData.uploading) {
        throw newConflictError(
          `Container ${body.containerId} video ${body.videoId} is not in uploading state anymore.`,
        );
      }
      if (
        row.videoTrackData.uploading.uploadSessionUrl !==
        originalUploadSessionUrl
      ) {
        throw newConflictError(
          `Upload sesison url of Container ${body.containerId} video ${body.videoId} has been changed from ${originalUploadSessionUrl} to ${row.videoTrackData.uploading.uploadSessionUrl}.`,
        );
      }
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoTrackStatement(
          {
            formatting: {
              gcsFilename: row.videoTrackData.uploading.gcsFilename,
            },
          },
          body.containerId,
          body.videoId,
        ),
        insertVideoFormattingTaskStatement(
          body.containerId,
          body.videoId,
          now,
          now,
        ),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
