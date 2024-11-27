import { EnumDescriptor } from '@selfage/message/descriptor';

export enum FailureReason {
  VIDEO_CODEC_REQUIRES_H264 = 1,
  AUDIO_CODEC_REQUIRES_AAC = 2,
  AUDIO_TOO_MANY_TRACKS = 3,
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
    name: 'AUDIO_TOO_MANY_TRACKS',
    value: 3,
  }]
}
