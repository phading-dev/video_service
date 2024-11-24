import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  getVideoContainer,
  insertGcsFileStatement,
  insertR2KeyStatement,
  insertVideoContainerStatement,
  insertVideoTrackStatement,
} from "../db/sql";
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
      let rows = await getVideoContainer(transaction, body.containerId);
      if (rows.length !== 0) {
        console.log(
          loggingPrefix,
          `Video container ${body.containerId} has been created.`,
        );
        return;
      }

      let r2Dirname = body.containerId;
      let videoId = body.containerId;
      let gcsFilename = body.containerId;
      let version = 0;
      await transaction.batchUpdate([
        insertVideoContainerStatement(body.containerId, {
          source: body.source,
          r2Dirname,
          version,
          totalBytes: 0
        }),
        insertR2KeyStatement(r2Dirname),
        insertR2KeyStatement(`${r2Dirname}/${version}`),
        insertVideoTrackStatement(body.containerId, videoId, {
          uploading: {
            gcsFilename,
          },
        }),
        insertGcsFileStatement(videoId),
      ]);
      await transaction.commit();
    });
    return {};
  }
}
