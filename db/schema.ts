import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
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
  r2TrackDirname?: string,
  r2TrackPlaylistFilename?: string,
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
  }, {
    name: 'r2TrackDirname',
    index: 4,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2TrackPlaylistFilename',
    index: 5,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface DoneState {
  totalBytes?: number,
  r2TrackDirname?: string,
  r2TrackPlaylistFilename?: string,
}

export let DONE_STATE: MessageDescriptor<DoneState> = {
  name: 'DoneState',
  fields: [{
    name: 'totalBytes',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2TrackDirname',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2TrackPlaylistFilename',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface VideoTrackData {
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: VideoTrackDoneState,
}

export let VIDEO_TRACK_DATA: MessageDescriptor<VideoTrackData> = {
  name: 'VideoTrackData',
  fields: [{
    name: 'uploading',
    index: 1,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 2,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 3,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 4,
    messageType: VIDEO_TRACK_DONE_STATE,
  }],
};

export interface AudioTrackData {
  name?: string,
  isDefault?: boolean,
  uploading?: ResumableUploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: DoneState,
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
    name: 'uploading',
    index: 3,
    messageType: RESUMABLE_UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 4,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 5,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 6,
    messageType: DONE_STATE,
  }],
};

export interface SubtitleTrackData {
  name?: string,
  isDefault?: boolean,
  uploading?: UploadingState,
  formatting?: FormattingState,
  failure?: FailureState,
  done?: DoneState,
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
    name: 'uploading',
    index: 3,
    messageType: UPLOADING_STATE,
  }, {
    name: 'formatting',
    index: 4,
    messageType: FORMATTING_STATE,
  }, {
    name: 'failure',
    index: 5,
    messageType: FAILURE_STATE,
  }, {
    name: 'done',
    index: 6,
    messageType: DONE_STATE,
  }],
};

export interface VideoContainerData {
  source?: Source,
  r2Dirname?: string,
  version?: number,
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
  }],
};
