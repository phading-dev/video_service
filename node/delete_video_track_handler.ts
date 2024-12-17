import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DeleteVideoTrackHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DeleteVideoTrackRequestBody,
  DeleteVideoTrackResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteVideoTrackHandler extends DeleteVideoTrackHandlerInterface {
  public static create(): DeleteVideoTrackHandler {
    return new DeleteVideoTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: DeleteVideoTrackRequestBody,
  ): Promise<DeleteVideoTrackResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(
        transaction,
        body.containerId,
      );
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let videoContainer = videoContainerRows[0].videoContainerData;
      let videoTrack = videoContainer.videoTracks.find(
        (videoTrack) => videoTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (!videoTrack) {
        throw newNotFoundError(
          `Video container ${body.containerId} video track ${body.r2TrackDirname} is not found.`,
        );
      }
      if (!videoTrack.committed) {
        throw newBadRequestError(
          `Video container ${body.containerId} video track ${body.r2TrackDirname} is not committed.`,
        );
      }
      videoTrack.staging = {
        toDelete: true,
      };
      await transaction.batchUpdate([
        updateVideoContainerStatement(body.containerId, videoContainer),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
