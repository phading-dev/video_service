import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
import { FailureReason, FAILURE_REASON } from './failure_reason';

export interface ResumableUploadingState {
  byteOffset?: number,
  contentLength?: number,
  contentType?: string,
}

export let RESUMABLE_UPLOADING_STATE: MessageDescriptor<ResumableUploadingState> = {
  name: 'ResumableUploadingState',
  fields: [{
    name: 'byteOffset',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'contentLength',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'contentType',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface FormattingState {
}

export let FORMATTING_STATE: MessageDescriptor<FormattingState> = {
  name: 'FormattingState',
  fields: [],
};

export interface ProcessingState {
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
  failures?: Array<FailureReason>,
}

export let PROCESSING_STATE: MessageDescriptor<ProcessingState> = {
  name: 'ProcessingState',
  fields: [{
    name: 'uploading',
    index: 1,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 2,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failures',
    index: 3,
    enumType: FAILURE_REASON,
    isArray: true,
  }],
};

export interface OneOfProcessingState {
  media?: ProcessingState,
  subtitle?: ProcessingState,
}

export let ONE_OF_PROCESSING_STATE: MessageDescriptor<OneOfProcessingState> = {
  name: 'OneOfProcessingState',
  fields: [{
    name: 'media',
    index: 1,
    messageType: PROCESSING_STATE,
  }, {
    name: 'subtitle',
    index: 2,
    messageType: PROCESSING_STATE,
  }],
};

export interface VideoTrackData {
  durationSec?: number,
  resolution?: string,
  totalBytes?: number,
}

export let VIDEO_TRACK_DATA: MessageDescriptor<VideoTrackData> = {
  name: 'VideoTrackData',
  fields: [{
    name: 'durationSec',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'resolution',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface VideoTrackDataStaging {
  toAdd?: VideoTrackData,
  toDelete?: boolean,
}

export let VIDEO_TRACK_DATA_STAGING: MessageDescriptor<VideoTrackDataStaging> = {
  name: 'VideoTrackDataStaging',
  fields: [{
    name: 'toAdd',
    index: 1,
    messageType: VIDEO_TRACK_DATA,
  }, {
    name: 'toDelete',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface VideoTrack {
  r2TrackDirname?: string,
  committed?: VideoTrackData,
  change?: VideoTrackDataStaging,
}

export let VIDEO_TRACK: MessageDescriptor<VideoTrack> = {
  name: 'VideoTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'committed',
    index: 2,
    messageType: VIDEO_TRACK_DATA,
  }, {
    name: 'change',
    index: 3,
    messageType: VIDEO_TRACK_DATA_STAGING,
  }],
};

export interface AudioTrackData {
  name?: string,
  isDefault?: boolean,
  totalBytes?: number,
}

export let AUDIO_TRACK_DATA: MessageDescriptor<AudioTrackData> = {
  name: 'AudioTrackData',
  fields: [{
    name: 'name',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'totalBytes',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface AudioTrackDataStaging {
  toAdd?: AudioTrackData,
  toDelete?: boolean,
}

export let AUDIO_TRACK_DATA_STAGING: MessageDescriptor<AudioTrackDataStaging> = {
  name: 'AudioTrackDataStaging',
  fields: [{
    name: 'toAdd',
    index: 1,
    messageType: AUDIO_TRACK_DATA,
  }, {
    name: 'toDelete',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface AudioTrack {
  r2TrackDirname?: string,
  committed?: AudioTrackData,
  staging?: AudioTrackDataStaging,
}

export let AUDIO_TRACK: MessageDescriptor<AudioTrack> = {
  name: 'AudioTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'committed',
    index: 2,
    messageType: AUDIO_TRACK_DATA,
  }, {
    name: 'staging',
    index: 3,
    messageType: AUDIO_TRACK_DATA_STAGING,
  }],
};

export interface SubtitleTrackData {
  name?: string,
  isDefault?: boolean,
  totalBytes?: number,
}

export let SUBTITLE_TRACK_DATA: MessageDescriptor<SubtitleTrackData> = {
  name: 'SubtitleTrackData',
  fields: [{
    name: 'name',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'totalBytes',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface SubtitleTrackDataStaging {
  toAdd?: SubtitleTrackData,
  toDelete?: boolean,
}

export let SUBTITLE_TRACK_DATA_STAGING: MessageDescriptor<SubtitleTrackDataStaging> = {
  name: 'SubtitleTrackDataStaging',
  fields: [{
    name: 'toAdd',
    index: 1,
    messageType: SUBTITLE_TRACK_DATA,
  }, {
    name: 'toDelete',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface SubtitleTrack {
  r2TrackDirname?: string,
  committed?: SubtitleTrackData,
  change?: SubtitleTrackDataStaging,
}

export let SUBTITLE_TRACK: MessageDescriptor<SubtitleTrack> = {
  name: 'SubtitleTrack',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'committed',
    index: 2,
    messageType: SUBTITLE_TRACK_DATA,
  }, {
    name: 'change',
    index: 3,
    messageType: SUBTITLE_TRACK_DATA_STAGING,
  }],
};

export interface WritingToFileState {
  version?: number,
}

export let WRITING_TO_FILE_STATE: MessageDescriptor<WritingToFileState> = {
  name: 'WritingToFileState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface SyncingState {
  version?: number,
}

export let SYNCING_STATE: MessageDescriptor<SyncingState> = {
  name: 'SyncingState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface SyncedState {
  version?: number,
}

export let SYNCED_STATE: MessageDescriptor<SyncedState> = {
  name: 'SyncedState',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
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
  masterPlaylist?: MasterPlaylistState,
  processing?: OneOfProcessingState,
  lastProcessingFailures?: Array<FailureReason>,
  videos?: Array<VideoTrack>,
  audios?: Array<AudioTrack>,
  subtitles?: Array<SubtitleTrack>,
}

export let VIDEO_CONTAINER: MessageDescriptor<VideoContainer> = {
  name: 'VideoContainer',
  fields: [{
    name: 'masterPlaylist',
    index: 1,
    messageType: MASTER_PLAYLIST_STATE,
  }, {
    name: 'processing',
    index: 2,
    messageType: ONE_OF_PROCESSING_STATE,
  }, {
    name: 'lastProcessingFailures',
    index: 3,
    enumType: FAILURE_REASON,
    isArray: true,
  }, {
    name: 'videos',
    index: 4,
    messageType: VIDEO_TRACK,
    isArray: true,
  }, {
    name: 'audios',
    index: 5,
    messageType: AUDIO_TRACK,
    isArray: true,
  }, {
    name: 'subtitles',
    index: 6,
    messageType: SUBTITLE_TRACK,
    isArray: true,
  }],
};
