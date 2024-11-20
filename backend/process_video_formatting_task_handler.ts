import { CLOUD_STORAGE } from "../common/cloud_storage";
import { GCS_VIDEO_BUCKET_NAME } from "../common/env_vars";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteVideoFormattingTaskStatement,
  getVideoTrack,
  updateVideoFormattingTaskStatement,
} from "../db/sql";
import {
  ProcessVideoFormattingTaskRequestBody,
  ProcessVideoFormattingTaskResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { Storage } from "@google-cloud/storage";
import { newNotFoundError } from "@selfage/http_error";
import { spawn } from "child_process";

export class ProcessVideoFormattingTaskHandler {
  public static create(): ProcessVideoFormattingTaskHandler {
    return new ProcessVideoFormattingTaskHandler(
      SPANNER_DATABASE,
      CLOUD_STORAGE,
      GCS_VIDEO_BUCKET_NAME,
      () => Date.now(),
    );
  }

  private static TIME_TO_DELAY_EXECUTION_MS = 5 * 60 * 1000;

  public constructor(
    private database: Database,
    private storage: Storage,
    private gcsVideoBucketName: string,
    private getNow: () => number,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: ProcessVideoFormattingTaskRequestBody,
  ): Promise<ProcessVideoFormattingTaskResponse> {
    let gcsFilename = await this.getPayloadAndDelayExecutionTime(body);
    this.startProcessing(loggingPrefix, body, gcsFilename);
    return {};
  }

  private async startProcessing(
    loggingPrefix: string,
    body: ProcessVideoFormattingTaskRequestBody,
    gcsFilename: string,
  ): Promise<void> {
    this.storage.bucket(this.gcsVideoBucketName).file(gcsFilename).baseUrl;
    spawn("ffprobe", ["-v", "error"]);
    spawn("ffmpeg", ["-i"]);
  }

  private async getPayloadAndDelayExecutionTime(
    body: ProcessVideoFormattingTaskRequestBody,
  ): Promise<string> {
    let gcsFilename: string;
    await this.database.runTransactionAsync(async (transaction) => {
      let rows = await getVideoTrack(
        transaction,
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
        throw newNotFoundError(
          `Container ${body.containerId} video ${body.videoId} is not in formatting state.`,
        );
      }
      gcsFilename = rows[0].videoTrackData.formatting.gcsFilename;
      await transaction.batchUpdate([
        updateVideoFormattingTaskStatement(
          this.getNow() +
            ProcessVideoFormattingTaskHandler.TIME_TO_DELAY_EXECUTION_MS,
          body.containerId,
          body.videoId,
        ),
      ]);
      await transaction.commit();
    });
    return gcsFilename;
  }
}
