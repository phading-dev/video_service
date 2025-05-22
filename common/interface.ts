import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';

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
