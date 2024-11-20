import { Source, SOURCE } from './source';
import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';
import { VideoContainer, VIDEO_CONTAINER } from './video_container';

export interface CreateVideoContainerRequestBody {
  source?: Source,
  containerId?: string,
}

export let CREATE_VIDEO_CONTAINER_REQUEST_BODY: MessageDescriptor<CreateVideoContainerRequestBody> = {
  name: 'CreateVideoContainerRequestBody',
  fields: [{
    name: 'source',
    index: 1,
    enumType: SOURCE,
  }, {
    name: 'containerId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface CreateVideoContainerResponse {
}

export let CREATE_VIDEO_CONTAINER_RESPONSE: MessageDescriptor<CreateVideoContainerResponse> = {
  name: 'CreateVideoContainerResponse',
  fields: [],
};

export interface GetVideoContainerRequestBody {
  containerId?: string,
}

export let GET_VIDEO_CONTAINER_REQUEST_BODY: MessageDescriptor<GetVideoContainerRequestBody> = {
  name: 'GetVideoContainerRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface GetVideoContainerResponse {
  videoContainer?: VideoContainer,
}

export let GET_VIDEO_CONTAINER_RESPONSE: MessageDescriptor<GetVideoContainerResponse> = {
  name: 'GetVideoContainerResponse',
  fields: [{
    name: 'videoContainer',
    index: 1,
    messageType: VIDEO_CONTAINER,
  }],
};

export interface StartVideoUploadRequestBody {
  containerId?: string,
  videoId?: string,
  totalBytes?: number,
}

export let START_VIDEO_UPLOAD_REQUEST_BODY: MessageDescriptor<StartVideoUploadRequestBody> = {
  name: 'StartVideoUploadRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'totalBytes',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface StartVideoUploadResponse {
  uploadSessionUrl?: string,
  bytesOffset?: number,
}

export let START_VIDEO_UPLOAD_RESPONSE: MessageDescriptor<StartVideoUploadResponse> = {
  name: 'StartVideoUploadResponse',
  fields: [{
    name: 'uploadSessionUrl',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'bytesOffset',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface CompleteVideoUploadRequestBody {
  containerId?: string,
  videoId?: string,
}

export let COMPLETE_VIDEO_UPLOAD_REQUEST_BODY: MessageDescriptor<CompleteVideoUploadRequestBody> = {
  name: 'CompleteVideoUploadRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface CompleteVideoUploadResponse {
}

export let COMPLETE_VIDEO_UPLOAD_RESPONSE: MessageDescriptor<CompleteVideoUploadResponse> = {
  name: 'CompleteVideoUploadResponse',
  fields: [],
};

export interface ProcessVideoFormattingTaskRequestBody {
  containerId?: string,
  videoId?: string,
}

export let PROCESS_VIDEO_FORMATTING_TASK_REQUEST_BODY: MessageDescriptor<ProcessVideoFormattingTaskRequestBody> = {
  name: 'ProcessVideoFormattingTaskRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface ProcessVideoFormattingTaskResponse {
}

export let PROCESS_VIDEO_FORMATTING_TASK_RESPONSE: MessageDescriptor<ProcessVideoFormattingTaskResponse> = {
  name: 'ProcessVideoFormattingTaskResponse',
  fields: [],
};
