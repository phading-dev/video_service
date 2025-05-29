import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  AudioTrack,
  SubtitleTrack,
  VideoTrack,
  WritingToFileState,
} from "../db/schema";
import {
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  insertVideoContainerWritingToFileTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { mergeVideoContainerStagingData } from "./common/merge_video_container_staging_data";
import { Database } from "@google-cloud/spanner";
import {
  MAX_NUM_OF_AUDIO_TRACKS,
  MAX_NUM_OF_SUBTITLE_TRACKS,
} from "@phading/constants/video";
import { CommitVideoContainerStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CommitVideoContainerStagingDataRequestBody,
  CommitVideoContainerStagingDataResponse,
} from "@phading/video_service_interface/node/interface";
import { ValidationError } from "@phading/video_service_interface/node/validation_error";
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
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerData, videoContainerAccountId } =
        videoContainerRows[0];

      let mergedResult = mergeVideoContainerStagingData(
        videoContainerData,
        body.videoContainer,
      );
      if (mergedResult.error) {
        error = mergedResult.error;
        console.log(
          loggingPrefix,
          `When finalizing video container ${body.containerId}, returning error ${ValidationError[error]} when merging staging data.`,
        );
        return;
      }

      let writingToFile: WritingToFileState = {};
      let versionOfSyncingTaskToDeleteOptional = new Array<number>();
      let versionOfWritingToFileToDeleteOptional = new Array<number>();
      if (videoContainerData.masterPlaylist.synced) {
        writingToFile = {
          version: videoContainerData.masterPlaylist.synced.version + 1,
          r2FilenamesToDelete: [
            videoContainerData.masterPlaylist.synced.r2Filename,
          ],
          r2DirnamesToDelete: [],
        };
      } else if (videoContainerData.masterPlaylist.syncing) {
        versionOfSyncingTaskToDeleteOptional.push(
          videoContainerData.masterPlaylist.syncing.version,
        );
        writingToFile = {
          version: videoContainerData.masterPlaylist.syncing.version + 1,
          r2FilenamesToDelete: [
            ...videoContainerData.masterPlaylist.syncing.r2FilenamesToDelete,
            videoContainerData.masterPlaylist.syncing.r2Filename,
          ],
          r2DirnamesToDelete:
            videoContainerData.masterPlaylist.syncing.r2DirnamesToDelete,
        };
      } else if (videoContainerData.masterPlaylist.writingToFile) {
        versionOfWritingToFileToDeleteOptional.push(
          videoContainerData.masterPlaylist.writingToFile.version,
        );
        writingToFile = {
          version: videoContainerData.masterPlaylist.writingToFile.version + 1,
          r2FilenamesToDelete:
            videoContainerData.masterPlaylist.writingToFile.r2FilenamesToDelete,
          r2DirnamesToDelete:
            videoContainerData.masterPlaylist.writingToFile.r2DirnamesToDelete,
        };
      }
      videoContainerData.masterPlaylist = {
        writingToFile,
      };

      let newVideoTracks = new Array<VideoTrack>();
      for (let videoTrack of videoContainerData.videoTracks) {
        if (videoTrack.staging?.toDelete) {
          writingToFile.r2DirnamesToDelete.push(videoTrack.r2TrackDirname);
        } else {
          videoTrack.committed = true;
          videoTrack.staging = undefined;
          newVideoTracks.push(videoTrack);
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
          audioTrack.committed =
            audioTrack.staging?.toAdd ?? audioTrack.committed;
          audioTrack.staging = undefined;
          if (audioTrack.committed.isDefault) {
            defaultAudioCount++;
          }
          newAudioTracks.push(audioTrack);
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
      for (let subtitleTrack of videoContainerData.subtitleTracks) {
        if (subtitleTrack.staging?.toDelete) {
          writingToFile.r2DirnamesToDelete.push(subtitleTrack.r2TrackDirname);
        } else {
          subtitleTrack.committed =
            subtitleTrack.staging?.toAdd ?? subtitleTrack.committed;
          subtitleTrack.staging = undefined;
          newSubtitleTracks.push(subtitleTrack);
        }
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
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        ...mergedResult.r2DirnamesToDelete.map((r2Dirname) =>
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...mergedResult.r2DirnamesToDelete.map((r2Dirname) =>
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        insertVideoContainerWritingToFileTaskStatement({
          containerId: body.containerId,
          version: writingToFile.version,
          retryCount: 0,
          executionTimeMs: now,
          createdTimeMs: now,
        }),
        ...versionOfWritingToFileToDeleteOptional.map((version) =>
          deleteVideoContainerWritingToFileTaskStatement({
            videoContainerWritingToFileTaskContainerIdEq: body.containerId,
            videoContainerWritingToFileTaskVersionEq: version,
          }),
        ),
        ...versionOfSyncingTaskToDeleteOptional.map((version) =>
          deleteVideoContainerSyncingTaskStatement({
            videoContainerSyncingTaskContainerIdEq: body.containerId,
            videoContainerSyncingTaskVersionEq: version,
          }),
        ),
      ]);
      await transaction.commit();
    });
    return {
      error,
    };
  }
}
