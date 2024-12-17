import { SyncVideoContainerRequestBody, SyncVideoContainerResponse, SYNC_VIDEO_CONTAINER } from './product_interface';
import { NodeClientInterface, NodeClientOptions } from '@selfage/service_descriptor/client_interface';

export function syncVideoContainer(
  client: NodeClientInterface,
  body: SyncVideoContainerRequestBody,
  options?: NodeClientOptions,
): Promise<SyncVideoContainerResponse> {
  return client.send(
    {
      descriptor: SYNC_VIDEO_CONTAINER,
      body,
    },
    options,
  );
}
