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
  MAX_NUM_OF_AUDIO_TRACKS,
  MAX_NUM_OF_SUBTITLE_TRACKS,
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
      let { videoContainerData } = videoContainerRows[0];
      let versionOfSyncingTaskToDeleteOptional = new Array<number>();
      let versionOfWritingToFileToDeleteOptional = new Array<number>();
      if (videoContainerData.masterPlaylist.synced) {
        videoContainerData.masterPlaylist = {
          writingToFile: {
            version: videoContainerData.masterPlaylist.synced.version + 1,
            r2FilenamesToDelete: [
              videoContainerData.masterPlaylist.synced.r2Filename,
            ],
            r2DirnamesToDelete: [],
          },
        };
      } else if (videoContainerData.masterPlaylist.syncing) {
        versionOfSyncingTaskToDeleteOptional.push(
          videoContainerData.masterPlaylist.syncing.version,
        );
        videoContainerData.masterPlaylist.syncing.r2FilenamesToDelete.push(
          videoContainerData.masterPlaylist.syncing.r2Filename,
        );
        videoContainerData.masterPlaylist = {
          writingToFile: {
            version: videoContainerData.masterPlaylist.syncing.version + 1,
            r2FilenamesToDelete:
              videoContainerData.masterPlaylist.syncing.r2FilenamesToDelete,
            r2DirnamesToDelete:
              videoContainerData.masterPlaylist.syncing.r2DirnamesToDelete,
          },
        };
      } else if (videoContainerData.masterPlaylist.writingToFile) {
        versionOfWritingToFileToDeleteOptional.push(
          videoContainerData.masterPlaylist.writingToFile.version,
        );
        videoContainerData.masterPlaylist = {
          writingToFile: {
            version:
              videoContainerData.masterPlaylist.writingToFile.version + 1,
            r2FilenamesToDelete:
              videoContainerData.masterPlaylist.writingToFile
                .r2FilenamesToDelete,
            r2DirnamesToDelete:
              videoContainerData.masterPlaylist.writingToFile
                .r2DirnamesToDelete,
          },
        };
      }

      let writingToFile = videoContainerData.masterPlaylist.writingToFile;
      let newVideoTracks = new Array<VideoTrack>();
      for (let videoTrack of videoContainerData.videoTracks) {
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
      videoContainerData.videoTracks = newVideoTracks;

      let newAudioTracks = new Array<AudioTrack>();
      let defaultAudioCount = 0;
      for (let audioTrack of videoContainerData.audioTracks) {
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
      if (newAudioTracks.length > MAX_NUM_OF_AUDIO_TRACKS) {
        error = ValidationError.TOO_MANY_AUDIO_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are too many audio tracks.`,
        );
        return;
      }
      videoContainerData.audioTracks = newAudioTracks;

      let newSubtitleTracks = new Array<SubtitleTrack>();
      let defaultSubtitleCount = 0;
      for (let subtitleTrack of videoContainerData.subtitleTracks) {
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
      if (newSubtitleTracks.length > MAX_NUM_OF_SUBTITLE_TRACKS) {
        error = ValidationError.TOO_MANY_SUBTITLE_TRACKS;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error that there are too many subtitle tracks.`,
        );
        return;
      }
      videoContainerData.subtitleTracks = newSubtitleTracks;

      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainerData),
        insertVideoContainerWritingToFileTaskStatement(
          videoContainerData.containerId,
          writingToFile.version,
          0,
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
