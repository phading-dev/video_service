import { SPANNER_DATABASE } from "../common/spanner_database";
import { listGcsFileDeleteTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListGcsFileDeleteTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListGcsFileDeleteTasksRequestBody,
  ListGcsFileDeleteTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListGcsFileDeleteTasksHandler extends ListGcsFileDeleteTasksHandlerInterface {
  public static create(): ListGcsFileDeleteTasksHandler {
    return new ListGcsFileDeleteTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListGcsFileDeleteTasksRequestBody,
  ): Promise<ListGcsFileDeleteTasksResponse> {
    let rows = await listGcsFileDeleteTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        gcsFilename: row.gcsFileDeleteTaskFilename,
        uploadSessionUrl: row.gcsFileDeleteTaskPayload.uploadSessionUrl,
      })),
    };
  }
}
