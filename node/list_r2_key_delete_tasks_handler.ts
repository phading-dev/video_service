import { SPANNER_DATABASE } from "../common/spanner_database";
import { listR2KeyDeleteTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListR2KeyDeleteTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListR2KeyDeleteTasksRequestBody,
  ListR2KeyDeleteTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListR2KeyDeleteTasksHandler extends ListR2KeyDeleteTasksHandlerInterface {
  public static create(): ListR2KeyDeleteTasksHandler {
    return new ListR2KeyDeleteTasksHandler(SPANNER_DATABASE, () => Date.now());
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: ListR2KeyDeleteTasksRequestBody,
  ): Promise<ListR2KeyDeleteTasksResponse> {
    let rows = await listR2KeyDeleteTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        key: row.r2KeyDeleteTaskKey,
      })),
    };
  }
}
