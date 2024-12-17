import { SyncVideoContainerRequestBody, SYNC_VIDEO_CONTAINER, SyncVideoContainerResponse } from './product_interface';
import { NodeHandlerInterface } from '@selfage/service_descriptor/handler_interface';

export abstract class SyncVideoContainerHandlerInterface implements NodeHandlerInterface {
  public descriptor = SYNC_VIDEO_CONTAINER;
  public abstract handle(
    loggingPrefix: string,
    body: SyncVideoContainerRequestBody,
  ): Promise<SyncVideoContainerResponse>;
}
