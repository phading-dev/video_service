import { MessageDescriptor, PrimitiveType } from '@selfage/message/descriptor';

export interface UploadingState {
}

export let UPLOADING_STATE: MessageDescriptor<UploadingState> = {
  name: 'UploadingState',
  fields: [],
};

export interface ResumableUploadingState {
  byteOffset?: number,
  totalBytes?: number,
}

export let RESUMABLE_UPLOADING_STATE: MessageDescriptor<ResumableUploadingState> = {
  name: 'ResumableUploadingState',
  fields: [{
    name: 'byteOffset',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface FormattingState {
}

export let FORMATTING_STATE: MessageDescriptor<FormattingState> = {
  name: 'FormattingState',
  fields: [],
};

export interface FailureState {
  reason?: string,
}

export let FAILURE_STATE: MessageDescriptor<FailureState> = {
  name: 'FailureState',
  fields: [{
    name: 'reason',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface VideoTrackDoneState {
  totalBytes?: number,
  durationSec?: number,
  resolution?: string,
}

export let VIDEO_TRACK_DONE_STATE: MessageDescriptor<VideoTrackDoneState> = {
  name: 'VideoTrackDoneState',
  fields: [{
    name: 'totalBytes',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'durationSec',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'resolution',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface DoneState {
  totalBytes?: number,
}

export let DONE_STATE: MessageDescriptor<DoneState> = {
  name: 'DoneState',
  fields: [{
    name: 'totalBytes',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface VideoTrack {
  videoId?: string,
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: VideoTrackDoneState,
}

export let VIDEO_TRACK: MessageDescriptor<VideoTrack> = {
  name: 'VideoTrack',
  fields: [{
    name: 'videoId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'uploading',
    index: 2,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 3,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 4,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 5,
    messageType: VIDEO_TRACK_DONE_STATE,
  }],
};

export interface AudioTrack {
  audioId?: string,
  name?: string,
  isDefault?: boolean,
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: DoneState,
}

export let AUDIO_TRACK: MessageDescriptor<AudioTrack> = {
  name: 'AudioTrack',
  fields: [{
    name: 'audioId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'name',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 3,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'uploading',
    index: 4,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 5,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 6,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 7,
    messageType: DONE_STATE,
  }],
};

export interface SubtitleTrack {
  subtitleId?: string,
  name?: string,
  isDefault?: boolean,
  uploading?: UploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: DoneState,
}

export let SUBTITLE_TRACK: MessageDescriptor<SubtitleTrack> = {
  name: 'SubtitleTrack',
  fields: [{
    name: 'subtitleId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'name',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'isDefault',
    index: 3,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'uploading',
    index: 4,
    messageType: UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 5,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 6,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 7,
    messageType: DONE_STATE,
  }],
};

export interface VideoContainer {
  version?: number,
  syncing?: boolean,
  videos?: Array<VideoTrack>,
  audios?: Array<AudioTrack>,
  subtitles?: Array<SubtitleTrack>,
}

export let VIDEO_CONTAINER: MessageDescriptor<VideoContainer> = {
  name: 'VideoContainer',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'syncing',
    index: 2,
    primitiveType: PrimitiveType.BOOLEAN,
  }, {
    name: 'videos',
    index: 3,
    messageType: VIDEO_TRACK,
    isArray: true,
  }, {
    name: 'audios',
    index: 4,
    messageType: AUDIO_TRACK,
    isArray: true,
  }, {
    name: 'subtitles',
    index: 5,
    messageType: SUBTITLE_TRACK,
    isArray: true,
  }],
};
