import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import {
  DeleteAudioTrackRequestBody,
  DeleteAudioTrackResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteAudioTrackHandler {
  public static create(): DeleteAudioTrackHandler {
    return new DeleteAudioTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

  public async handle(
    loggingPrefix: string,
    body: DeleteAudioTrackRequestBody,
  ): Promise<DeleteAudioTrackResponse> {
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
      let audioTrack = videoContainer.audioTracks.find(
        (audioTrack) => audioTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (!audioTrack) {
        throw newNotFoundError(
          `Video container ${body.containerId} audio track ${body.r2TrackDirname} is not found.`,
        );
      }
      if (!audioTrack.committed) {
        throw newBadRequestError(
          `Video container ${body.containerId} audio track ${body.r2TrackDirname} is not committed.`,
        );
      }
      audioTrack.staging = {
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
