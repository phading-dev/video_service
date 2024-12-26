import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, insertVideoContainerStatement } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { CreateVideoContainerHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  CreateVideoContainerRequestBody,
  CreateVideoContainerResponse,
} from "@phading/video_service_interface/node/interface";

export class CreateVideoContainerHandler extends CreateVideoContainerHandlerInterface {
  public static create(): CreateVideoContainerHandler {
    return new CreateVideoContainerHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: CreateVideoContainerRequestBody,
  ): Promise<CreateVideoContainerResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(
        transaction,
        body.containerId,
      );
      if (videoContainerRows.length !== 0) {
        console.log(
          loggingPrefix,
          `Video container ${body.containerId} has been created.`,
        );
        return;
      }

      let r2RootDirname = `show${body.containerId}`;
      await transaction.batchUpdate([
        insertVideoContainerStatement({
          containerId: body.containerId,
          seasonId: body.seasonId,
          episodeId: body.episodeId,
          r2RootDirname,
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
    return {};
  }
}
