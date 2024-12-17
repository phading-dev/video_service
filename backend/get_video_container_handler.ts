import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer } from "../db/sql";
import {
  GetVideoContainerRequestBody,
  GetVideoContainerResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { newNotFoundError } from "@selfage/http_error";

export class GetVideoContainerHandler {
  public static create(): GetVideoContainerHandler {
    return new GetVideoContainerHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

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
