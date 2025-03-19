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
        masterPlaylist: videoContainerData.masterPlaylist,
        processing: videoContainerData.processing
          ? {
              media: videoContainerData.processing.media
                ? {
                    uploading: videoContainerData.processing.media.uploading,
                    formatting: videoContainerData.processing.media.formatting,
                  }
                : undefined,
              subtitle: videoContainerData.processing.subtitle
                ? {
                    uploading: videoContainerData.processing.subtitle.uploading,
                    formatting:
                      videoContainerData.processing.subtitle.formatting,
                  }
                : undefined,
            }
          : undefined,
        lastProcessingFailures: videoContainerData.lastProcessingFailures,
        videos: videoContainerData.videoTracks.map(
          (videoTrack): VideoTrack => ({
            r2TrackDirname: videoTrack.r2TrackDirname,
            staging: videoTrack.staging
              ? {
                  toAdd: videoTrack.staging.toAdd
                    ? {
                        durationSec: videoTrack.staging.toAdd.durationSec,
                        resolution: videoTrack.staging.toAdd.resolution,
                        totalBytes: videoTrack.staging.toAdd.totalBytes,
                      }
                    : undefined,
                  toDelete: videoTrack.staging.toDelete,
                }
              : undefined,
            committed: videoTrack.committed
              ? {
                  durationSec: videoTrack.committed.durationSec,
                  resolution: videoTrack.committed.resolution,
                  totalBytes: videoTrack.committed.totalBytes,
                }
              : undefined,
          }),
        ),
        audios: videoContainerData.audioTracks.map(
          (audioTrack): AudioTrack => ({
            r2TrackDirname: audioTrack.r2TrackDirname,
            staging: audioTrack.staging
              ? {
                  toAdd: audioTrack.staging.toAdd
                    ? {
                        name: audioTrack.staging.toAdd.name,
                        isDefault: audioTrack.staging.toAdd.isDefault,
                        totalBytes: audioTrack.staging.toAdd.totalBytes,
                      }
                    : undefined,
                  toDelete: audioTrack.staging.toDelete,
                }
              : undefined,
            committed: audioTrack.committed
              ? {
                  name: audioTrack.committed.name,
                  isDefault: audioTrack.committed.isDefault,
                  totalBytes: audioTrack.committed.totalBytes,
                }
              : undefined,
          }),
        ),
        subtitles: videoContainerData.subtitleTracks.map(
          (subtitleTrack): SubtitleTrack => ({
            r2TrackDirname: subtitleTrack.r2TrackDirname,
            staging: subtitleTrack.staging
              ? {
                  toAdd: subtitleTrack.staging.toAdd
                    ? {
                        name: subtitleTrack.staging.toAdd.name,
                        isDefault: subtitleTrack.staging.toAdd.isDefault,
                        totalBytes: subtitleTrack.staging.toAdd.totalBytes,
                      }
                    : undefined,
                  toDelete: subtitleTrack.staging.toDelete,
                }
              : undefined,
            committed: subtitleTrack.committed
              ? {
                  name: subtitleTrack.committed.name,
                  isDefault: subtitleTrack.committed.isDefault,
                  totalBytes: subtitleTrack.committed.totalBytes,
                }
              : undefined,
          }),
        ),
      },
    };
  }
}
