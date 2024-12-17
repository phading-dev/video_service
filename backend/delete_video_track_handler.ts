import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import {
  DeleteVideoTrackRequestBody,
  DeleteVideoTrackResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteVideoTrackHandler {
  public static create(): DeleteVideoTrackHandler {
    return new DeleteVideoTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

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
