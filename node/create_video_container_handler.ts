import { SPANNER_DATABASE } from "../common/spanner_database";
import { insertVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { CreateVideoContainerHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CreateVideoContainerRequestBody,
  CreateVideoContainerResponse,
} from "@phading/video_service_interface/node/interface";

export class CreateVideoContainerHandler extends CreateVideoContainerHandlerInterface {
  public static create(): CreateVideoContainerHandler {
    return new CreateVideoContainerHandler(SPANNER_DATABASE, () => Date.now());
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: CreateVideoContainerRequestBody,
  ): Promise<CreateVideoContainerResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        insertVideoContainerStatement({
          containerId: body.videoContainerId,
          seasonId: body.seasonId,
          episodeId: body.episodeId,
          accountId: body.accountId,
          createdTimeMs: this.getNow(),
          data: {
            r2RootDirname: body.videoContainerId,
            masterPlaylist: {
              synced: {
                version: 0,
              },
            },
            videoTracks: [],
            audioTracks: [],
            subtitleTracks: [],
          },
        }),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
