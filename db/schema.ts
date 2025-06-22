import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
import { VideoTrackStagingData, VIDEO_TRACK_STAGING_DATA, AudioTrackStagingData, AUDIO_TRACK_STAGING_DATA, SubtitleTrackStagingData, SUBTITLE_TRACK_STAGING_DATA } from '@phading/video_service_interface/node/video_container_staging_data';
import { AudioTrackMutableData, AUDIO_TRACK_MUTABLE_DATA, SubtitleTrackMutableData, SUBTITLE_TRACK_MUTABLE_DATA } from '@phading/video_service_interface/node/track_mutable_data';
import { LastProcessingFailure, LAST_PROCESSING_FAILURE } from '@phading/video_service_interface/node/last_processing_failure';

export interface ResumableUploadingState {
  gcsFilename?: string,
  uploadSessionUrl?: string,
  contentLength?: number,
  fileExt?: string,
  md5?: string,
}

export let RESUMABLE_UPLOADING_STATE: MessageDescriptor<ResumableUploadingState> = {
  name: 'ResumableUploadingState',
  fields: [{
    name: 'gcsFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'uploadSessionUrl',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'contentLength',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'fileExt',
    index: 4,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'md5',
    index: 5,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface FormattingState {
  gcsFilename?: string,
}

export let FORMATTING_STATE: MessageDescriptor<FormattingState> = {
  name: 'FormattingState',
  fields: [{
    name: 'gcsFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface VideoInfo {
  durationSec?: number,
  resolution?: string,
}

export let VIDEO_INFO: MessageDescriptor<VideoInfo> = {
  name: 'VideoInfo',
  fields: [{
    name: 'durationSec',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'resolution',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface MediaUploadingState {
  gcsDirname?: string,
  gcsVideoDirname?: string,
  videoInfo?: VideoInfo,
  gcsAudioDirnames?: Array<string>,
}

export let MEDIA_UPLOADING_STATE: MessageDescriptor<MediaUploadingState> = {
  name: 'MediaUploadingState',
  fields: [{
    name: 'gcsDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsVideoDirname',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoInfo',
    index: 3,
    messageType: VIDEO_INFO,
  }, {
    name: 'gcsAudioDirnames',
    index: 4,
    primitiveType: PrimitiveType.STRING,
    isArray: true,
  }],
};

export interface OneOfProcessingState {
  uploading?: ResumableUploadingState,
  mediaFormatting?: FormattingState,
  mediaUploading?: MediaUploadingState,
  subtitleFormatting?: FormattingState,
}

export let ONE_OF_PROCESSING_STATE: MessageDescriptor<OneOfProcessingState> = {
  name: 'OneOfProcessingState',
  fields: [{
    name: 'uploading',
    index: 1,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'mediaFormatting',
    index: 2,
    messageType: FORMATTING_STATE,
  }, {
    name: 'mediaUploading',
    index: 3,
    messageType: MEDIA_UPLOADING_STATE,
  }, {
    name: 'subtitleFormatting',
    index: 4,
    messageType: FORMATTING_STATE,
  }],
};

export interface VideoTrack {
  r2TrackDirname?: string,
  durationSec?: number,
  resolution?: string,
  totalBytes?: number,
  committed?: boolean,
  staging?: VideoTrackStagingData,
}

export let VIDEO_TRACK: MessageDescriptor<VideoTrack> = {
  name: 'VideoTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'durationSec',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'resolution',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'committed',
    index: 5,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'staging',
    index: 6,
    messageType: VIDEO_TRACK_STAGING_DATA,
  }],
};

export interface AudioTrack {
  r2TrackDirname?: string,
  totalBytes?: number,
  committed?: AudioTrackMutableData,
  staging?: AudioTrackStagingData,
}

export let AUDIO_TRACK: MessageDescriptor<AudioTrack> = {
  name: 'AudioTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'committed',
    index: 3,
    messageType: AUDIO_TRACK_MUTABLE_DATA,
  }, {
    name: 'staging',
    index: 4,
    messageType: AUDIO_TRACK_STAGING_DATA,
  }],
};

export interface SubtitleTrack {
  r2TrackDirname?: string,
  totalBytes?: number,
  committed?: SubtitleTrackMutableData,
  staging?: SubtitleTrackStagingData,
}

export let SUBTITLE_TRACK: MessageDescriptor<SubtitleTrack> = {
  name: 'SubtitleTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'committed',
    index: 3,
    messageType: SUBTITLE_TRACK_MUTABLE_DATA,
  }, {
    name: 'staging',
    index: 4,
    messageType: SUBTITLE_TRACK_STAGING_DATA,
  }],
};

export interface WritingToFileState {
  version?: number,
  r2FilenamesToDelete?: Array<string>,
  r2DirnamesToDelete?: Array<string>,
}

export let WRITING_TO_FILE_STATE: MessageDescriptor<WritingToFileState> = {
  name: 'WritingToFileState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2FilenamesToDelete',
    index: 2,
    primitiveType: PrimitiveType.STRING,
    isArray: true,
  }, {
    name: 'r2DirnamesToDelete',
    index: 3,
    primitiveType: PrimitiveType.STRING,
    isArray: true,
  }],
};

export interface SyncingState {
  version?: number,
  r2Filename?: string,
  r2FilenamesToDelete?: Array<string>,
  r2DirnamesToDelete?: Array<string>,
}

export let SYNCING_STATE: MessageDescriptor<SyncingState> = {
  name: 'SyncingState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2Filename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2FilenamesToDelete',
    index: 3,
    primitiveType: PrimitiveType.STRING,
    isArray: true,
  }, {
    name: 'r2DirnamesToDelete',
    index: 4,
    primitiveType: PrimitiveType.STRING,
    isArray: true,
  }],
};

export interface SyncedState {
  version?: number,
  r2Filename?: string,
}

export let SYNCED_STATE: MessageDescriptor<SyncedState> = {
  name: 'SyncedState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2Filename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface MasterPlaylistState {
  writingToFile?: WritingToFileState,
  syncing?: SyncingState,
  synced?: SyncedState,
}

export let MASTER_PLAYLIST_STATE: MessageDescriptor<MasterPlaylistState> = {
  name: 'MasterPlaylistState',
  fields: [{
    name: 'writingToFile',
    index: 1,
    messageType: WRITING_TO_FILE_STATE,
  }, {
    name: 'syncing',
    index: 2,
    messageType: SYNCING_STATE,
  }, {
    name: 'synced',
    index: 3,
    messageType: SYNCED_STATE,
  }],
};

export interface VideoContainer {
  r2RootDirname?: string,
  masterPlaylist?: MasterPlaylistState,
  processing?: OneOfProcessingState,
  lastProcessingFailure?: LastProcessingFailure,
  videoTracks?: Array<VideoTrack>,
  audioTracks?: Array<AudioTrack>,
  subtitleTracks?: Array<SubtitleTrack>,
}

export let VIDEO_CONTAINER: MessageDescriptor<VideoContainer> = {
  name: 'VideoContainer',
  fields: [{
    name: 'r2RootDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'masterPlaylist',
    index: 2,
    messageType: MASTER_PLAYLIST_STATE,
  }, {
    name: 'processing',
    index: 3,
    messageType: ONE_OF_PROCESSING_STATE,
  }, {
    name: 'lastProcessingFailure',
    index: 4,
    messageType: LAST_PROCESSING_FAILURE,
  }, {
    name: 'videoTracks',
    index: 5,
    messageType: VIDEO_TRACK,
    isArray: true,
  }, {
    name: 'audioTracks',
    index: 6,
    messageType: AUDIO_TRACK,
    isArray: true,
  }, {
    name: 'subtitleTracks',
    index: 7,
    messageType: SUBTITLE_TRACK,
    isArray: true,
  }],
};

export interface UploadedRecordingTaskPayload {
  accountId?: string,
  totalBytes?: number,
}

export let UPLOADED_RECORDING_TASK_PAYLOAD: MessageDescriptor<UploadedRecordingTaskPayload> = {
  name: 'UploadedRecordingTaskPayload',
  fields: [{
    name: 'accountId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface StorageStartRecordingTaskPayload {
  accountId?: string,
  totalBytes?: number,
  startTimeMs?: number,
}

export let STORAGE_START_RECORDING_TASK_PAYLOAD: MessageDescriptor<StorageStartRecordingTaskPayload> = {
  name: 'StorageStartRecordingTaskPayload',
  fields: [{
    name: 'accountId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'startTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface StorageEndRecordingTaskPayload {
  accountId?: string,
  endTimeMs?: number,
}

export let STORAGE_END_RECORDING_TASK_PAYLOAD: MessageDescriptor<StorageEndRecordingTaskPayload> = {
  name: 'StorageEndRecordingTaskPayload',
  fields: [{
    name: 'accountId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'endTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};
