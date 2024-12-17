import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
import { NodeRemoteCallDescriptor } from '@selfage/service_descriptor';

export interface AudioTrack {
  name?: string,
  isDefault?: boolean,
}

export let AUDIO_TRACK: MessageDescriptor<AudioTrack> = {
  name: 'AudioTrack',
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
  name?: string,
  isDefault?: boolean,
}

export let SUBTITLE_TRACK: MessageDescriptor<SubtitleTrack> = {
  name: 'SubtitleTrack',
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

export interface VideoContainer {
  version?: number,
  r2RootDirname?: string,
  r2MasterPlaylistFilename?: string,
  durationSec?: number,
  resolution?: string,
  audioTracks?: Array<AudioTrack>,
  subtitleTracks?: Array<SubtitleTrack>,
}

export let VIDEO_CONTAINER: MessageDescriptor<VideoContainer> = {
  name: 'VideoContainer',
  fields: [{
    name: 'version',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2RootDirname',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2MasterPlaylistFilename',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'durationSec',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'resolution',
    index: 5,
    primitiveType: PrimitiveType.STRING,
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

export interface SyncVideoContainerRequestBody {
  showId?: string,
  containerId?: string,
  container?: VideoContainer,
}

export let SYNC_VIDEO_CONTAINER_REQUEST_BODY: MessageDescriptor<SyncVideoContainerRequestBody> = {
  name: 'SyncVideoContainerRequestBody',
  fields: [{
    name: 'showId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'containerId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'container',
    index: 3,
    messageType: VIDEO_CONTAINER,
  }],
};

export interface SyncVideoContainerResponse {
}

export let SYNC_VIDEO_CONTAINER_RESPONSE: MessageDescriptor<SyncVideoContainerResponse> = {
  name: 'SyncVideoContainerResponse',
  fields: [],
};

export let SYNC_VIDEO_CONTAINER: NodeRemoteCallDescriptor = {
  name: "SyncVideoContainer",
  path: "undefined",
  body: {
    messageType: SYNC_VIDEO_CONTAINER_REQUEST_BODY,
  },
  response: {
    messageType: SYNC_VIDEO_CONTAINER_RESPONSE,
  },
}
