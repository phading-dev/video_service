import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { mergeVideoContainerStagingData } from "./common/merge_video_container_staging_data";
import { Database } from "@google-cloud/spanner";
import { SaveVideoContainerStagingDataHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  SaveVideoContainerStagingDataRequestBody,
  SaveVideoContainerStagingDataResponse,
} from "@phading/video_service_interface/node/interface";
import { ValidationError } from "@phading/video_service_interface/node/validation_error";
import { newNotFoundError } from "@selfage/http_error";

export class SaveVideoContainerStagingDataHandler extends SaveVideoContainerStagingDataHandlerInterface {
  public static create(): SaveVideoContainerStagingDataHandler {
    return new SaveVideoContainerStagingDataHandler(SPANNER_DATABASE, () =>
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
    body: SaveVideoContainerStagingDataRequestBody,
  ): Promise<SaveVideoContainerStagingDataResponse> {
    let error: ValidationError;
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(transaction, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        throw newNotFoundError(
          `Video container ${body.containerId} is not found.`,
        );
      }
      let { videoContainerData, videoContainerAccountId } =
        videoContainerRows[0];
      let mergedResult = mergeVideoContainerStagingData(
        videoContainerData,
        body.videoContainer,
      );
      if (mergedResult.error) {
        error = mergedResult.error;
        return;
      }

      let now = this.getNow();
      await transaction.batchUpdate([
        updateVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
          setData: videoContainerData,
        }),
        ...mergedResult.r2DirnamesToDelete.map((r2Dirname) =>
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
        ...mergedResult.r2DirnamesToDelete.map((r2Dirname) =>
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
    return {
      error,
    };
  }
}
