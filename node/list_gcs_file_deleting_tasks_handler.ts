import { SPANNER_DATABASE } from "../common/spanner_database";
import { listGcsFileDeletingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListGcsFileDeletingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListGcsFileDeletingTasksRequestBody,
  ListGcsFileDeletingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListGcsFileDeletingTasksHandler extends ListGcsFileDeletingTasksHandlerInterface {
  public static create(): ListGcsFileDeletingTasksHandler {
    return new ListGcsFileDeletingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListGcsFileDeletingTasksRequestBody,
  ): Promise<ListGcsFileDeletingTasksResponse> {
    let rows = await listGcsFileDeletingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        gcsFilename: row.gcsFileDeletingTaskFilename,
        uploadSessionUrl: row.gcsFileDeletingTaskUploadSessionUrl,
      })),
    };
  }
}
