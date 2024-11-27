import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
import { FailureReason, FAILURE_REASON } from '../failure_reason';
import { Source, SOURCE } from '../source';

export interface ResumableUploadingState {
  gcsFilename?: string,
  uploadSessionUrl?: string,
  totalBytes?: number,
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
    name: 'totalBytes',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface UploadingState {
  gcsFilename?: string,
}

export let UPLOADING_STATE: MessageDescriptor<UploadingState> = {
  name: 'UploadingState',
  fields: [{
    name: 'gcsFilename',
    index: 1,
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

export interface MediaProcessingData {
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
}

export let MEDIA_PROCESSING_DATA: MessageDescriptor<MediaProcessingData> = {
  name: 'MediaProcessingData',
  fields: [{
    name: 'uploading',
    index: 1,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 2,
    messageType: FORMATTING_STATE,
  }],
};

export interface SubtitleProcessingData {
  uploading?: UploadingState,
  formatting?: FormattingState,
}

export let SUBTITLE_PROCESSING_DATA: MessageDescriptor<SubtitleProcessingData> = {
  name: 'SubtitleProcessingData',
  fields: [{
    name: 'uploading',
    index: 1,
    messageType: UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 2,
    messageType: FORMATTING_STATE,
  }],
};

export interface ProcessingData {
  media?: MediaProcessingData,
  subtitle?: SubtitleProcessingData,
  lastFailures?: Array<FailureReason>,
}

export let PROCESSING_DATA: MessageDescriptor<ProcessingData> = {
  name: 'ProcessingData',
  fields: [{
    name: 'media',
    index: 1,
    messageType: MEDIA_PROCESSING_DATA,
  }, {
    name: 'subtitle',
    index: 2,
    messageType: SUBTITLE_PROCESSING_DATA,
  }, {
    name: 'lastFailures',
    index: 3,
    enumType: FAILURE_REASON,
    isArray: true,
  }],
};

export interface VideoTrackData {
  r2TrackDirname?: string,
  durationSec?: number,
  resolution?: string,
  totalBytes?: number,
}

export let VIDEO_TRACK_DATA: MessageDescriptor<VideoTrackData> = {
  name: 'VideoTrackData',
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
  }],
};

export interface VideoTrack {
  data?: VideoTrackData,
  toAdd?: VideoTrackData,
  toRemove?: boolean,
}

export let VIDEO_TRACK: MessageDescriptor<VideoTrack> = {
  name: 'VideoTrack',
  fields: [{
    name: 'data',
    index: 1,
    messageType: VIDEO_TRACK_DATA,
  }, {
    name: 'toAdd',
    index: 2,
    messageType: VIDEO_TRACK_DATA,
  }, {
    name: 'toRemove',
    index: 3,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface AudioTrackData {
  name?: string,
  isDefault?: boolean,
  r2TrackDirname?: string,
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
    name: 'r2TrackDirname',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface ChangeAudioTrack {
  name?: string,
  isDefault?: boolean,
}

export let CHANGE_AUDIO_TRACK: MessageDescriptor<ChangeAudioTrack> = {
  name: 'ChangeAudioTrack',
  fields: [{
    name: 'name',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface AudioTrack {
  data?: AudioTrackData,
  toAdd?: AudioTrackData,
  toChange?: ChangeAudioTrack,
  toRemove?: boolean,
}

export let AUDIO_TRACK: MessageDescriptor<AudioTrack> = {
  name: 'AudioTrack',
  fields: [{
    name: 'data',
    index: 1,
    messageType: AUDIO_TRACK_DATA,
  }, {
    name: 'toAdd',
    index: 2,
    messageType: AUDIO_TRACK_DATA,
  }, {
    name: 'toChange',
    index: 3,
    messageType: CHANGE_AUDIO_TRACK,
  }, {
    name: 'toRemove',
    index: 4,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface SubtitleTrackData {
  name?: string,
  isDefault?: boolean,
  r2TrackDirname?: string,
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
    name: 'r2TrackDirname',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface ChangeSubtitleTrack {
  name?: string,
  isDefault?: boolean,
}

export let CHANGE_SUBTITLE_TRACK: MessageDescriptor<ChangeSubtitleTrack> = {
  name: 'ChangeSubtitleTrack',
  fields: [{
    name: 'name',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface SubtitleTrack {
  data?: SubtitleTrackData,
  toAdd?: SubtitleTrackData,
  toChange?: ChangeSubtitleTrack,
  toRemove?: boolean,
}

export let SUBTITLE_TRACK: MessageDescriptor<SubtitleTrack> = {
  name: 'SubtitleTrack',
  fields: [{
    name: 'data',
    index: 1,
    messageType: SUBTITLE_TRACK_DATA,
  }, {
    name: 'toAdd',
    index: 2,
    messageType: SUBTITLE_TRACK_DATA,
  }, {
    name: 'toChange',
    index: 3,
    messageType: CHANGE_SUBTITLE_TRACK,
  }, {
    name: 'toRemove',
    index: 4,
    primitiveType: PrimitiveType.BOOLEAN,
  }],
};

export interface VideoContainerData {
  source?: Source,
  r2Dirname?: string,
  version?: number,
  processing?: ProcessingData,
  videoTracks?: Array<VideoTrack>,
  audioTracks?: Array<AudioTrack>,
  subtitleTracks?: Array<SubtitleTrack>,
}

export let VIDEO_CONTAINER_DATA: MessageDescriptor<VideoContainerData> = {
  name: 'VideoContainerData',
  fields: [{
    name: 'source',
    index: 1,
    enumType: SOURCE,
  }, {
    name: 'r2Dirname',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'version',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'processing',
    index: 4,
    messageType: PROCESSING_DATA,
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
