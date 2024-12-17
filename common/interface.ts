import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';

export interface StartResumableUploadingRequestBody {
  containerId?: string,
  contentLength?: number,
  contentType?: string,
}

export let START_RESUMABLE_UPLOADING_REQUEST_BODY: MessageDescriptor<StartResumableUploadingRequestBody> = {
  name: 'StartResumableUploadingRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
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

export interface StartResumableUploadingResponse {
  uploadSessionUrl?: string,
  byteOffset?: number,
}

export let START_RESUMABLE_UPLOADING_RESPONSE: MessageDescriptor<StartResumableUploadingResponse> = {
  name: 'StartResumableUploadingResponse',
  fields: [{
    name: 'uploadSessionUrl',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'byteOffset',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export interface CompleteResumableUploadingRequestBody {
  containerId?: string,
  uploadSessionUrl?: string,
}

export let COMPLETE_RESUMABLE_UPLOADING_REQUEST_BODY: MessageDescriptor<CompleteResumableUploadingRequestBody> = {
  name: 'CompleteResumableUploadingRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'uploadSessionUrl',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface CompleteResumableUploadingResponse {
}

export let COMPLETE_RESUMABLE_UPLOADING_RESPONSE: MessageDescriptor<CompleteResumableUploadingResponse> = {
  name: 'CompleteResumableUploadingResponse',
  fields: [],
};

export interface CancelResumableUploadingRequestBody {
  containerId?: string,
}

export let CANCEL_RESUMABLE_UPLOADING_REQUEST_BODY: MessageDescriptor<CancelResumableUploadingRequestBody> = {
  name: 'CancelResumableUploadingRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface CancelResumableUploadingResponse {
}

export let CANCEL_RESUMABLE_UPLOADING_RESPONSE: MessageDescriptor<CancelResumableUploadingResponse> = {
  name: 'CancelResumableUploadingResponse',
  fields: [],
};

export interface CancelFormattingRequestBody {
  containerId?: string,
}

export let CANCEL_FORMATTING_REQUEST_BODY: MessageDescriptor<CancelFormattingRequestBody> = {
  name: 'CancelFormattingRequestBody',
  fields: [{
    name: 'containerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export interface CancelFormattingResponse {
}

export let CANCEL_FORMATTING_RESPONSE: MessageDescriptor<CancelFormattingResponse> = {
  name: 'CancelFormattingResponse',
  fields: [],
};
