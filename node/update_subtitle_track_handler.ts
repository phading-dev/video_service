import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, updateVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { UpdateSubtitleTrackHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  UpdateSubtitleTrackRequestBody,
  UpdateSubtitleTrackResponse,
} from "@phading/video_service_interface/node/interface";
import { newNotFoundError } from "@selfage/http_error";

export class UpdateSubtitleTrackHandler extends UpdateSubtitleTrackHandlerInterface {
  public static create(): UpdateSubtitleTrackHandler {
    return new UpdateSubtitleTrackHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: UpdateSubtitleTrackRequestBody,
  ): Promise<UpdateSubtitleTrackResponse> {
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
      if (!subtitleTrack.staging) {
        subtitleTrack.staging = {
          toAdd: {
            ...subtitleTrack.committed,
          },
        };
      }
      subtitleTrack.staging.toAdd.name = body.name;
      subtitleTrack.staging.toAdd.isDefault = body.isDefault;

      await transaction.batchUpdate([
        updateVideoContainerStatement(videoContainer),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
