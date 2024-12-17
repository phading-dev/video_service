import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import {
  UpdateAudioTrackRequestBody,
  UpdateAudioTrackResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";
import { newNotFoundError } from "@selfage/http_error";

export class UpdateAudioTrackHandler {
  public static create(): UpdateAudioTrackHandler {
    return new UpdateAudioTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

  public async handle(
    loggingPrefix: string,
    body: UpdateAudioTrackRequestBody,
  ): Promise<UpdateAudioTrackResponse> {
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
      if (!audioTrack.staging) {
        audioTrack.staging = {
          toAdd: {
            ...audioTrack.committed,
          },
        };
      }
      audioTrack.staging.toAdd.name = body.name;
      audioTrack.staging.toAdd.isDefault = body.isDefault;

      await transaction.batchUpdate([
        updateVideoContainerStatement(body.containerId, videoContainer),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
