import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DeleteAudioTrackHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DeleteAudioTrackRequestBody,
  DeleteAudioTrackResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteAudioTrackHandler extends DeleteAudioTrackHandlerInterface {
  public static create(): DeleteAudioTrackHandler {
    return new DeleteAudioTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: DeleteAudioTrackRequestBody,
  ): Promise<DeleteAudioTrackResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerData } = videoContainerRows[0];
      let audioTrack = videoContainerData.audioTracks.find(
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
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
