import { EnumDescriptor } from '@selfage/message/descriptor';

export enum FailureReason {
  VIDEO_CODEC_REQUIRES_H264 = 1,
  AUDIO_CODEC_REQUIRES_AAC = 2,
  VIDEO_NEEDS_AT_LEAST_ONE_TRACK = 3,
  AUDIO_TOO_MANY_TRACKS = 4,
}

export let FAILURE_REASON: EnumDescriptor<FailureReason> = {
  name: 'FailureReason',
  values: [{
    name: 'VIDEO_CODEC_REQUIRES_H264',
    value: 1,
  }, {
    name: 'AUDIO_CODEC_REQUIRES_AAC',
    value: 2,
  }, {
    name: 'VIDEO_NEEDS_AT_LEAST_ONE_TRACK',
    value: 3,
  }, {
    name: 'AUDIO_TOO_MANY_TRACKS',
    value: 4,
  }]
}
