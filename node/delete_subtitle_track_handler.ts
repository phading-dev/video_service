import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DeleteSubtitleTrackHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DeleteSubtitleTrackRequestBody,
  DeleteSubtitleTrackResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DeleteSubtitleTrackHandler extends DeleteSubtitleTrackHandlerInterface {
  public static create(): DeleteSubtitleTrackHandler {
    return new DeleteSubtitleTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: DeleteSubtitleTrackRequestBody,
  ): Promise<DeleteSubtitleTrackResponse> {
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
      let subtitleTrack = videoContainerData.subtitleTracks.find(
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
