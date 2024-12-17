import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import {
  DeleteSubtitleTrackRequestBody,
  DeleteSubtitleTrackResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteSubtitleTrackHandler {
  public static create(): DeleteSubtitleTrackHandler {
    return new DeleteSubtitleTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

  public async handle(
    loggingPrefix: string,
    body: DeleteSubtitleTrackRequestBody,
  ): Promise<DeleteSubtitleTrackResponse> {
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
      let subtitleTrack = videoContainer.subtitleTracks.find(
        (subtitleTrack) => subtitleTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (!subtitleTrack) {
        throw newNotFoundError(
          `Video container ${body.containerId} subtitle track ${body.r2TrackDirname} is not found.`,
        );
      }
      if (!subtitleTrack.committed) {
        throw newBadRequestError(
          `Video container ${body.containerId} subtitle track ${body.r2TrackDirname} is not committed.`,
        );
      }
      subtitleTrack.staging = {
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
