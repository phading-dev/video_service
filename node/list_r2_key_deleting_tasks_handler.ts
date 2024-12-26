import { SPANNER_DATABASE } from "../common/spanner_database";
import { listR2KeyDeletingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListR2KeyDeletingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListR2KeyDeletingTasksRequestBody,
  ListR2KeyDeletingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListR2KeyDeletingTasksHandler extends ListR2KeyDeletingTasksHandlerInterface {
  public static create(): ListR2KeyDeletingTasksHandler {
    return new ListR2KeyDeletingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListR2KeyDeletingTasksRequestBody,
  ): Promise<ListR2KeyDeletingTasksResponse> {
    let rows = await listR2KeyDeletingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        key: row.r2KeyDeletingTaskKey,
      })),
    };
  }
}
