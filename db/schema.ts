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

export interface FailureState {
  reasons?: Array<FailureReason>,
}

export let FAILURE_STATE: MessageDescriptor<FailureState> = {
  name: 'FailureState',
  fields: [{
    name: 'reasons',
    index: 1,
    enumType: FAILURE_REASON,
    isArray: true,
  }],
};

export interface VideoTrackDoneState {
  r2TrackDirname?: string,
  durationSec?: number,
  resolution?: string,
  totalBytes?: number,
}

export let VIDEO_TRACK_DONE_STATE: MessageDescriptor<VideoTrackDoneState> = {
  name: 'VideoTrackDoneState',
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

export interface DoneState {
  r2TrackDirname?: string,
  totalBytes?: number,
}

export let DONE_STATE: MessageDescriptor<DoneState> = {
  name: 'DoneState',
  fields: [{
    name: 'r2TrackDirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
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
  totalBytes?: number,
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
    name: 'totalBytes',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }],
};
