import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DropVideoTrackStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DropVideoTrackStagingDataRequestBody,
  DropVideoTrackStagingDataResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DropVideoTrackStagingDataHandler extends DropVideoTrackStagingDataHandlerInterface {
  public static create(): DropVideoTrackStagingDataHandler {
    return new DropVideoTrackStagingDataHandler(SPANNER_DATABASE, () =>
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
    body: DropVideoTrackStagingDataRequestBody,
  ): Promise<DropVideoTrackStagingDataResponse> {
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
      let index = videoContainer.videoTracks.findIndex(
        (videoTrack) => videoTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (index === -1) {
        throw newNotFoundError(
          `Video container ${body.containerId} video track ${body.r2TrackDirname} is not found.`,
        );
      }
      let videoTrack = videoContainer.videoTracks[index];
      if (!videoTrack.staging) {
        throw newBadRequestError(
          `Video container ${body.containerId} video track ${body.r2TrackDirname} is not in staging.`,
        );
      }
      videoTrack.staging = undefined;
      let r2DirnameToDeleteOptional = new Array<string>();
      if (!videoTrack.committed) {
        r2DirnameToDeleteOptional.push(videoTrack.r2TrackDirname);
        videoContainer.videoTracks.splice(index, 1);
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
