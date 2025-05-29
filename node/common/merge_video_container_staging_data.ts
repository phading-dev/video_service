import {
  AudioTrack,
  SubtitleTrack,
  VideoContainer,
  VideoTrack,
} from "../../db/schema";
import { ValidationError } from "@phading/video_service_interface/node/validation_error";
import { VideoContainerStagingData } from "@phading/video_service_interface/node/video_container_staging_data";

export function mergeVideoContainerStagingData(
  videoContainer: VideoContainer,
  stagingData: VideoContainerStagingData,
): {
  error?: ValidationError;
  r2DirnamesToDelete?: Array<string>;
} {
  if (
    videoContainer.videoTracks.length !== stagingData.videos.length ||
    videoContainer.audioTracks.length !== stagingData.audios.length ||
    videoContainer.subtitleTracks.length !== stagingData.subtitles.length
  ) {
    return { error: ValidationError.TRACK_MISMATCH };
  }
  let r2DirnamesToDelete = new Array<string>();
  let mergedVideoTracks = new Array<VideoTrack>();
  for (let i = 0; i < videoContainer.videoTracks.length; i++) {
    let videoTrack = videoContainer.videoTracks[i];
    let stagingVideoTrack = stagingData.videos[i];
    if (videoTrack.r2TrackDirname !== stagingVideoTrack.r2TrackDirname) {
      return { error: ValidationError.TRACK_MISMATCH };
    }
    videoTrack.staging = stagingVideoTrack.staging;
    if (!videoTrack.committed && !videoTrack.staging) {
      r2DirnamesToDelete.push(videoTrack.r2TrackDirname);
    } else {
      mergedVideoTracks.push(videoTrack);
    }
  }
  videoContainer.videoTracks = mergedVideoTracks;

  let mergedAudioTracks = new Array<AudioTrack>();
  for (let i = 0; i < videoContainer.audioTracks.length; i++) {
    let audioTrack = videoContainer.audioTracks[i];
    let stagingAudioTrack = stagingData.audios[i];
    if (audioTrack.r2TrackDirname !== stagingAudioTrack.r2TrackDirname) {
      return { error: ValidationError.TRACK_MISMATCH };
    }
    audioTrack.staging = stagingAudioTrack.staging;
    if (!audioTrack.committed && !audioTrack.staging) {
      r2DirnamesToDelete.push(audioTrack.r2TrackDirname);
    } else {
      mergedAudioTracks.push(audioTrack);
    }
  }
  videoContainer.audioTracks = mergedAudioTracks;

  let mergedSubtitleTracks = new Array<SubtitleTrack>();
  for (let i = 0; i < videoContainer.subtitleTracks.length; i++) {
    let subtitleTrack = videoContainer.subtitleTracks[i];
    let stagingSubtitleTrack = stagingData.subtitles[i];
    if (subtitleTrack.r2TrackDirname !== stagingSubtitleTrack.r2TrackDirname) {
      return { error: ValidationError.TRACK_MISMATCH };
    }
    subtitleTrack.staging = stagingSubtitleTrack.staging;
    if (!subtitleTrack.committed && !subtitleTrack.staging) {
      r2DirnamesToDelete.push(subtitleTrack.r2TrackDirname);
    } else {
      mergedSubtitleTracks.push(subtitleTrack);
    }
  }
  videoContainer.subtitleTracks = mergedSubtitleTracks;

  return {
    r2DirnamesToDelete,
  };
}
