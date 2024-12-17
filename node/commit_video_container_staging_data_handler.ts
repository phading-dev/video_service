import { SPANNER_DATABASE } from "../common/spanner_database";
import { AudioTrack, SubtitleTrack, VideoTrack } from "../db/schema";
import {
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertVideoContainerWritingToFileTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import {
  AUDIO_TRACKS_LIMIT,
  SUBTITLE_TRACKS_LIMIT,
} from "@phading/constants/video";
import { CommitVideoContainerStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CommitVideoContainerStagingDataRequestBody,
  CommitVideoContainerStagingDataResponse,
  ValidationError,
} from "@phading/video_service_interface/node/interface";
import { newNotFoundError } from "@selfage/http_error";

export class CommitVideoContainerStagingDataHandler extends CommitVideoContainerStagingDataHandlerInterface {
  public static create(): CommitVideoContainerStagingDataHandler {
    return new CommitVideoContainerStagingDataHandler(SPANNER_DATABASE, () =>
      Date.now(),
    );
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: CommitVideoContainerStagingDataRequestBody,
  ): Promise<CommitVideoContainerStagingDataResponse> {
    let error: ValidationError;
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
      let videoContainer = videoContainerRows[0].videoContainerData;
      let versionOfSyncingTaskToDeleteOptional = new Array<number>();
      let versionOfWritingToFileToDeleteOptional = new Array<number>();
      if (videoContainer.masterPlaylist.synced) {
        videoContainer.masterPlaylist = {
          writingToFile: {
            version: videoContainer.masterPlaylist.synced.version + 1,
            r2FilenamesToDelete: [
              videoContainer.masterPlaylist.synced.r2Filename,
            ],
            r2DirnamesToDelete: [],
          },
        };
      } else if (videoContainer.masterPlaylist.syncing) {
        versionOfSyncingTaskToDeleteOptional.push(
          videoContainer.masterPlaylist.syncing.version,
        );
        videoContainer.masterPlaylist.syncing.r2FilenamesToDelete.push(
          videoContainer.masterPlaylist.syncing.r2Filename,
        );
        videoContainer.masterPlaylist = {
          writingToFile: {
            version: videoContainer.masterPlaylist.syncing.version + 1,
            r2FilenamesToDelete:
              videoContainer.masterPlaylist.syncing.r2FilenamesToDelete,
            r2DirnamesToDelete:
              videoContainer.masterPlaylist.syncing.r2DirnamesToDelete,
          },
        };
      } else if (videoContainer.masterPlaylist.writingToFile) {
        versionOfWritingToFileToDeleteOptional.push(
          videoContainer.masterPlaylist.writingToFile.version,
        );
        videoContainer.masterPlaylist = {
          writingToFile: {
            version: videoContainer.masterPlaylist.writingToFile.version + 1,
            r2FilenamesToDelete:
              videoContainer.masterPlaylist.writingToFile.r2FilenamesToDelete,
            r2DirnamesToDelete:
              videoContainer.masterPlaylist.writingToFile.r2DirnamesToDelete,
          },
        };
      }

      let writingToFile = videoContainer.masterPlaylist.writingToFile;
      let newVideoTracks = new Array<VideoTrack>();
      for (let videoTrack of videoContainer.videoTracks) {
        if (videoTrack.staging?.toDelete) {
          writingToFile.r2DirnamesToDelete.push(videoTrack.r2TrackDirname);
        } else {
          newVideoTracks.push({
            r2TrackDirname: videoTrack.r2TrackDirname,
            committed: videoTrack.staging?.toAdd ?? videoTrack.committed,
          });
        }
      }
      if (newVideoTracks.length === 0) {
        error = ValidationError.NO_VIDEO_TRACK;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there is no video track.`,
        );
        return;
      }
      if (newVideoTracks.length > 1) {
        error = ValidationError.MORE_THAN_ONE_VIDEO_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are multiple video tracks.`,
        );
        return;
      }
      videoContainer.videoTracks = newVideoTracks;

      let newAudioTracks = new Array<AudioTrack>();
      let defaultAudioCount = 0;
      for (let audioTrack of videoContainer.audioTracks) {
        if (audioTrack.staging?.toDelete) {
          writingToFile.r2DirnamesToDelete.push(audioTrack.r2TrackDirname);
        } else {
          let committed = audioTrack.staging?.toAdd ?? audioTrack.committed;
          if (committed.isDefault) {
            defaultAudioCount++;
          }
          newAudioTracks.push({
            r2TrackDirname: audioTrack.r2TrackDirname,
            committed,
          });
        }
      }
      if (newAudioTracks.length > 0 && defaultAudioCount === 0) {
        error = ValidationError.NO_DEFAULT_AUDIO_TRACK;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there is no default audio track.`,
        );
        return;
      }
      if (defaultAudioCount > 1) {
        error = ValidationError.MORE_THAN_ONE_DEFAULT_AUDIO_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are multiple default audio tracks.`,
        );
        return;
      }
      if (newAudioTracks.length > AUDIO_TRACKS_LIMIT) {
        error = ValidationError.TOO_MANY_AUDIO_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are too many audio tracks.`,
        );
        return;
      }
      videoContainer.audioTracks = newAudioTracks;

      let newSubtitleTracks = new Array<SubtitleTrack>();
      let defaultSubtitleCount = 0;
      for (let subtitleTrack of videoContainer.subtitleTracks) {
        if (subtitleTrack.staging?.toDelete) {
          writingToFile.r2DirnamesToDelete.push(subtitleTrack.r2TrackDirname);
        } else {
          let committed =
            subtitleTrack.staging?.toAdd ?? subtitleTrack.committed;
          if (committed.isDefault) {
            defaultSubtitleCount++;
          }
          newSubtitleTracks.push({
            r2TrackDirname: subtitleTrack.r2TrackDirname,
            committed,
          });
        }
      }
      if (newSubtitleTracks.length > 0 && defaultSubtitleCount === 0) {
        error = ValidationError.NO_DEFAULT_SUBTITLE_TRACK;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there is no default subtitle track.`,
        );
        return;
      }
      if (defaultSubtitleCount > 1) {
        error = ValidationError.MORE_THAN_ONE_DEFAULT_SUBTITLE_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are multiple default subtitle tracks.`,
        );
        return;
      }
      if (newSubtitleTracks.length > SUBTITLE_TRACKS_LIMIT) {
        error = ValidationError.TOO_MANY_SUBTITLE_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are too many subtitle tracks.`,
        );
        return;
      }
      videoContainer.subtitleTracks = newSubtitleTracks;

      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(body.containerId, videoContainer),
        insertVideoContainerWritingToFileTaskStatement(
          body.containerId,
          videoContainer.masterPlaylist.writingToFile.version,
          now,
          now,
        ),
        ...versionOfWritingToFileToDeleteOptional.map((version) =>
          deleteVideoContainerWritingToFileTaskStatement(
            body.containerId,
            version,
          ),
        ),
        ...versionOfSyncingTaskToDeleteOptional.map((version) =>
          deleteVideoContainerSyncingTaskStatement(body.containerId, version),
        ),
      ]);
      await transaction.commit();
    });
    return {
      success: !error,
      error,
    };
  }
}
