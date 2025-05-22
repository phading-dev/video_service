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
              uploading: videoContainerData.processing.uploading,
              mediaFormatting: videoContainerData.processing.mediaFormatting,
              subtitleFormatting:
                videoContainerData.processing.subtitleFormatting,
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
            staging: videoTrack.staging
              ? {
                  toAdd: videoTrack.staging.toAdd,
                  toDelete: videoTrack.staging.toDelete,
                }
              : undefined,
          }),
        ),
        audios: videoContainerData.audioTracks.map(
          (audioTrack): AudioTrack => ({
            r2TrackDirname: audioTrack.r2TrackDirname,
            totalBytes: audioTrack.totalBytes,
            staging: audioTrack.staging
              ? {
                  toAdd: audioTrack.staging.toAdd
                    ? {
                        name: audioTrack.staging.toAdd.name,
                        isDefault: audioTrack.staging.toAdd.isDefault,
                      }
                    : undefined,
                  toDelete: audioTrack.staging.toDelete,
                }
              : undefined,
            committed: audioTrack.committed
              ? {
                  name: audioTrack.committed.name,
                  isDefault: audioTrack.committed.isDefault,
                }
              : undefined,
          }),
        ),
        subtitles: videoContainerData.subtitleTracks.map(
          (subtitleTrack): SubtitleTrack => ({
            r2TrackDirname: subtitleTrack.r2TrackDirname,
            totalBytes: subtitleTrack.totalBytes,
            staging: subtitleTrack.staging
              ? {
                  toAdd: subtitleTrack.staging.toAdd
                    ? {
                        name: subtitleTrack.staging.toAdd.name,
                      }
                    : undefined,
                  toDelete: subtitleTrack.staging.toDelete,
                }
              : undefined,
            committed: subtitleTrack.committed
              ? {
                  name: subtitleTrack.committed.name,
                }
              : undefined,
          }),
        ),
      },
    };
  }
}
