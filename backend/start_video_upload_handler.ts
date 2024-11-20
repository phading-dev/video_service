import { GCS_VIDEO_BUCKET_NAME } from "../common/env_vars";
import { CLOUD_STORAGE_CLIENT, CloudStorageClient } from "../common/gcs_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoTrack, updateVideoTrackStatement } from "../db/sql";
import {
  StartVideoUploadRequestBody,
  StartVideoUploadResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import {
  newBadRequestError,
  newConflictError,
  newNotFoundError,
} from "@selfage/http_error";

export class StartVideoUploadHandler {
  public static create(): StartVideoUploadHandler {
    return new StartVideoUploadHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE_CLIENT,
      GCS_VIDEO_BUCKET_NAME,
    );
  }

  private static VIDEO_CONTENT_TYPE = "video/mp4";

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
    private gcsVideoBucketName: string,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: StartVideoUploadRequestBody,
  ): Promise<StartVideoUploadResponse> {
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
    let { urlValid, byteOffset } =
      await this.gcsClient.checkResumableUploadProgress(
        body.totalBytes,
        originalUploadSessionUrl,
      );
    let newUploadSessionUrl: string;
    if (!urlValid) {
      newUploadSessionUrl = await this.gcsClient.createResumableUploadUrl(
        this.gcsVideoBucketName,
        row.videoTrackData.uploading.gcsFilename,
        StartVideoUploadHandler.VIDEO_CONTENT_TYPE,
        body.totalBytes,
      );
    } else {
      newUploadSessionUrl = originalUploadSessionUrl;
    }

    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getVideoTrack(
        this.database,
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
      await transaction.batchUpdate([
        updateVideoTrackStatement(
          {
            uploading: {
              gcsFilename: row.videoTrackData.uploading.gcsFilename,
              uploadSessionUrl: newUploadSessionUrl,
              totalBytes: body.totalBytes,
            },
          },
          body.containerId,
          body.videoId,
        ),
      ]);
      await transaction.commit();
    });

    return {
      uploadSessionUrl: newUploadSessionUrl,
      bytesOffset: byteOffset,
    };
  }
}
