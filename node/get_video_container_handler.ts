import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { GetVideoContainerHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  GetVideoContainerRequestBody,
  GetVideoContainerResponse,
} from "@phading/video_service_interface/node/interface";
import {
  AudioTrack,
  SubtitleTrack,
  VideoTrack,
} from "@phading/video_service_interface/node/video_container";
import { newNotFoundError } from "@selfage/http_error";

export class GetVideoContainerHandler extends GetVideoContainerHandlerInterface {
  public static create(): GetVideoContainerHandler {
    return new GetVideoContainerHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetVideoContainerRequestBody,
  ): Promise<GetVideoContainerResponse> {
    let videoContainerRows = await getVideoContainer(this.database, {
      videoContainerContainerIdEq: body.containerId,
    });
    if (videoContainerRows.length === 0) {
      throw newNotFoundError(
        `Video container ${body.containerId} is not found.`,
      );
    }
    let { videoContainerData } = videoContainerRows[0];
    return {
      videoContainer: {
        masterPlaylist: {
          committing:
            videoContainerData.masterPlaylist.syncing ||
            videoContainerData.masterPlaylist.writingToFile
              ? {
                  version:
                    videoContainerData.masterPlaylist.syncing?.version ??
                    videoContainerData.masterPlaylist.writingToFile?.version,
                }
              : undefined,
          synced: videoContainerData.masterPlaylist.synced
            ? {
                version: videoContainerData.masterPlaylist.synced.version,
              }
            : undefined,
        },
        processing: videoContainerData.processing
          ? {
              uploading: videoContainerData.processing.uploading
                ? {
                    fileExt: videoContainerData.processing.uploading.fileExt,
                    md5: videoContainerData.processing.uploading.md5,
                  }
                : undefined,
              mediaFormatting:
                (videoContainerData.processing.mediaFormatting ??
                videoContainerData.processing.mediaUploading)
                  ? {}
                  : undefined,
              subtitleFormatting: videoContainerData.processing
                .subtitleFormatting
                ? {}
                : undefined,
            }
          : undefined,
        lastProcessingFailure: videoContainerData.lastProcessingFailure,
        videos: videoContainerData.videoTracks.map(
          (videoTrack): VideoTrack => ({
            r2TrackDirname: videoTrack.r2TrackDirname,
            durationSec: videoTrack.durationSec,
            resolution: videoTrack.resolution,
            totalBytes: videoTrack.totalBytes,
            committed: videoTrack.committed,
            staging: videoTrack.staging,
          }),
        ),
        audios: videoContainerData.audioTracks.map(
          (audioTrack): AudioTrack => ({
            r2TrackDirname: audioTrack.r2TrackDirname,
            totalBytes: audioTrack.totalBytes,
            committed: audioTrack.committed,
            staging: audioTrack.staging,
          }),
        ),
        subtitles: videoContainerData.subtitleTracks.map(
          (subtitleTrack): SubtitleTrack => ({
            r2TrackDirname: subtitleTrack.r2TrackDirname,
            totalBytes: subtitleTrack.totalBytes,
            committed: subtitleTrack.committed,
            staging: subtitleTrack.staging,
          }),
        ),
      },
    };
  }
}
