import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { GetVideoContainerHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  GetVideoContainerRequestBody,
  GetVideoContainerResponse,
} from "@phading/video_service_interface/node/interface";
import { newNotFoundError } from "@selfage/http_error";

export class GetVideoContainerHandler extends GetVideoContainerHandlerInterface {
  public static create(): GetVideoContainerHandler {
    return new GetVideoContainerHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetVideoContainerRequestBody,
  ): Promise<GetVideoContainerResponse> {
    let videoContainerRows = await getVideoContainer(
      this.database,
      body.containerId,
    );
    if (videoContainerRows.length === 0) {
      throw newNotFoundError(
        `Video container ${body.containerId} is not found.`,
      );
    }
    let videoContainer = videoContainerRows[0].videoContainerData;
    return {
      videoContainer,
    };
  }
}
