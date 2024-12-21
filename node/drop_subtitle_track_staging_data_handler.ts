import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DropSubtitleTrackStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DropSubtitleTrackStagingDataRequestBody,
  DropSubtitleTrackStagingDataResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DropSubtitleTrackStagingDataHandler extends DropSubtitleTrackStagingDataHandlerInterface {
  public static create(): DropSubtitleTrackStagingDataHandler {
    return new DropSubtitleTrackStagingDataHandler(SPANNER_DATABASE, () =>
      Date.now(),
    );
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: DropSubtitleTrackStagingDataRequestBody,
  ): Promise<DropSubtitleTrackStagingDataResponse> {
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
      let index = videoContainer.subtitleTracks.findIndex(
        (subtitleTrack) => subtitleTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (index === -1) {
        throw newNotFoundError(
          `Video container ${body.containerId} subtitle track ${body.r2TrackDirname} is not found.`,
        );
      }
      let subtitleTrack = videoContainer.subtitleTracks[index];
      if (!subtitleTrack.staging) {
        throw newBadRequestError(
          `Video container ${body.containerId} subtitle track ${body.r2TrackDirname} is not in staging.`,
        );
      }
      subtitleTrack.staging = undefined;
      let r2DirnameToDeleteOptional = new Array<string>();
      if (!subtitleTrack.committed) {
        r2DirnameToDeleteOptional.push(subtitleTrack.r2TrackDirname);
        videoContainer.subtitleTracks.splice(index, 1);
      }
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement(body.containerId, videoContainer),
        ...r2DirnameToDeleteOptional.map((r2Dirname) =>
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${r2Dirname}`,
            now,
            now,
          ),
        ),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
