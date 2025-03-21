import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { DropAudioTrackStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DropAudioTrackStagingDataRequestBody,
  DropAudioTrackStagingDataResponse,
} from "@phading/video_service_interface/node/interface";
import { newBadRequestError, newNotFoundError } from "@selfage/http_error";

export class DropAudioTrackStagingDataHandler extends DropAudioTrackStagingDataHandlerInterface {
  public static create(): DropAudioTrackStagingDataHandler {
    return new DropAudioTrackStagingDataHandler(SPANNER_DATABASE, () =>
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
    body: DropAudioTrackStagingDataRequestBody,
  ): Promise<DropAudioTrackStagingDataResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerAccountId, videoContainerData } =
        videoContainerRows[0];
      let index = videoContainerData.audioTracks.findIndex(
        (audioTrack) => audioTrack.r2TrackDirname === body.r2TrackDirname,
      );
      if (index === -1) {
        throw newNotFoundError(
          `Video container ${body.containerId} audio track ${body.r2TrackDirname} is not found.`,
        );
      }
      let audioTrack = videoContainerData.audioTracks[index];
      if (!audioTrack.staging) {
        throw newBadRequestError(
          `Video container ${body.containerId} audio track ${body.r2TrackDirname} is not in staging.`,
        );
      }
      audioTrack.staging = undefined;
      let r2DirnameToDeleteOptional = new Array<string>();
      if (!audioTrack.committed) {
        r2DirnameToDeleteOptional.push(audioTrack.r2TrackDirname);
        videoContainerData.audioTracks.splice(index, 1);
      }
      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        ...r2DirnameToDeleteOptional.map((r2Dirname) =>
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
        ...r2DirnameToDeleteOptional.map((r2Dirname) =>
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${r2Dirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        ),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
