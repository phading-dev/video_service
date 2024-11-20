import { CLOUD_STORAGE_CLIENT, CloudStorageClient } from "../common/gcs_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GetAllAudioTracksRow,
  GetAllSubtitleTracksRow,
  GetAllVideoTracksRow,
  checkVideoContainerSyncingTask,
  getAllAudioTracks,
  getAllSubtitleTracks,
  getAllVideoTracks,
  getVideoContainer,
} from "../db/sql";
import {
  GetVideoContainerRequestBody,
  GetVideoContainerResponse,
} from "../interface";
import { AudioTrack, SubtitleTrack, VideoTrack } from "../video_container";
import { Database } from "@google-cloud/spanner";
import {
  newInternalServerErrorError,
  newNotFoundError,
} from "@selfage/http_error";

export class GetVideoContainerHandler {
  public static create(): GetVideoContainerHandler {
    return new GetVideoContainerHandler(SPANNER_DATABASE, CLOUD_STORAGE_CLIENT);
  }

  public constructor(
    private database: Database,
    private gcsClient: CloudStorageClient,
  ) {}

  public async handle(
    loggingPrefix: string,
    body: GetVideoContainerRequestBody,
  ): Promise<GetVideoContainerResponse> {
    let [
      videoContainerRows,
      videoTracks,
      audioTracks,
      subtitleTracks,
      syncingQueues,
    ] = await Promise.all([
      getVideoContainer(this.database, body.containerId),
      getAllVideoTracks(this.database, body.containerId),
      getAllAudioTracks(this.database, body.containerId),
      getAllSubtitleTracks(this.database, body.containerId),
      checkVideoContainerSyncingTask(this.database, body.containerId),
    ]);
    if (videoContainerRows.length === 0) {
      throw newNotFoundError(
        `Video container ${body.containerId} is not found.`,
      );
    }
    let videoContainerRow = videoContainerRows[0];
    return {
      videoContainer: {
        version: videoContainerRow.videoContainerData.version,
        syncing: syncingQueues.length > 0,
        videos: await Promise.all(
          videoTracks.map((track) => this.toVideoTrackResponse(track)),
        ),
        audios: await Promise.all(
          audioTracks.map((track) => this.toAudioTrackResponse(track)),
        ),
        subtitles: subtitleTracks.map((track) =>
          GetVideoContainerHandler.toSubtitleTrackResponse(track),
        ),
      },
    };
  }

  private async toVideoTrackResponse(
    row: GetAllVideoTracksRow,
  ): Promise<VideoTrack> {
    let trackData = row.videoTrackData;
    let ret: VideoTrack = {
      videoId: row.videoTrackVideoId,
    };
    if (trackData.uploading) {
      let { byteOffset } = await this.gcsClient.checkResumableUploadProgress(
        trackData.uploading.totalBytes,
        trackData.uploading.uploadSessionUrl,
      );
      ret.uploading = {
        totalBytes: trackData.uploading.totalBytes,
        byteOffset: byteOffset,
      };
    } else if (trackData.formatting) {
      ret.formatting = {};
    } else if (trackData.failure) {
      ret.failure = {
        reason: trackData.failure.reason,
      };
    } else if (trackData.done) {
      ret.done = {
        durationSec: trackData.done.durationSec,
        resolution: trackData.done.resolution,
        totalBytes: trackData.done.totalBytes,
      };
    } else {
      throw newInternalServerErrorError(
        `New state not handled: ${JSON.stringify(row)}.`,
      );
    }
    return ret;
  }

  private async toAudioTrackResponse(
    row: GetAllAudioTracksRow,
  ): Promise<AudioTrack> {
    let trackData = row.audioTrackData;
    let ret: AudioTrack = {
      audioId: row.audioTrackAudioId,
      name: trackData.name,
      isDefault: trackData.isDefault,
    };
    if (trackData.uploading) {
      let { byteOffset } = await this.gcsClient.checkResumableUploadProgress(
        trackData.uploading.totalBytes,
        trackData.uploading.uploadSessionUrl,
      );
      ret.uploading = {
        totalBytes: trackData.uploading.totalBytes,
        byteOffset: byteOffset,
      };
    } else if (trackData.formatting) {
      ret.formatting = {};
    } else if (trackData.failure) {
      ret.failure = {
        reason: trackData.failure.reason,
      };
    } else if (trackData.done) {
      ret.done = {
        totalBytes: trackData.done.totalBytes,
      };
    } else {
      throw newInternalServerErrorError(
        `New state not handled: ${JSON.stringify(row)}.`,
      );
    }
    return ret;
  }

  private static toSubtitleTrackResponse(
    row: GetAllSubtitleTracksRow,
  ): SubtitleTrack {
    let trackData = row.subtitleTrackData;
    let ret: SubtitleTrack = {
      subtitleId: row.subtitleTrackSubtitleId,
      name: trackData.name,
      isDefault: trackData.isDefault,
    };
    if (trackData.uploading) {
      ret.uploading = {};
    } else if (trackData.formatting) {
      ret.formatting = {};
    } else if (trackData.failure) {
      ret.failure = {
        reason: trackData.failure.reason,
      };
    } else if (trackData.done) {
      ret.done = {
        totalBytes: trackData.done.totalBytes,
      };
    } else {
      throw newInternalServerErrorError(
        `New state not handled: ${JSON.stringify(row)}.`,
      );
    }
    return ret;
  }
}
