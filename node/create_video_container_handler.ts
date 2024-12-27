import crypto = require("crypto");
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
    return new CreateVideoContainerHandler(SPANNER_DATABASE, () =>
      crypto.randomUUID(),
    );
  }

  public constructor(
    private database: Database,
    private generateUuid: () => string,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: CreateVideoContainerRequestBody,
  ): Promise<CreateVideoContainerResponse> {
    let containerId = this.generateUuid();
    await this.database.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        insertVideoContainerStatement({
          containerId,
          seasonId: body.seasonId,
          episodeId: body.episodeId,
          r2RootDirname: containerId,
          masterPlaylist: {
            synced: {
              version: 0,
              r2Filename: "0", // # Not really in use.
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        }),
      ]);
      await transaction.commit();
    });
    return {
      containerId,
    };
  }
}
