import { SPANNER_DATABASE } from "../common/spanner_database";
import { getVideoContainer, insertVideoContainerStatement } from "../db/sql";
import {
  CreateVideoContainerRequestBody,
  CreateVideoContainerResponse,
} from "../interface";
import { Database } from "@google-cloud/spanner";

export class CreateVideoContainerHandler {
  public static create(): CreateVideoContainerHandler {
    return new CreateVideoContainerHandler(SPANNER_DATABASE);
  }

  public constructor(private database: Database) {}

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
        insertVideoContainerStatement(body.containerId, {
          showId: body.showId,
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
