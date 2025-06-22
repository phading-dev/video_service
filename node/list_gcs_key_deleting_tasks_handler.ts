import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingGcsKeyDeletingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListGcsKeyDeletingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListGcsKeyDeletingTasksRequestBody,
  ListGcsKeyDeletingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListGcsKeyDeletingTasksHandler extends ListGcsKeyDeletingTasksHandlerInterface {
  public static create(): ListGcsKeyDeletingTasksHandler {
    return new ListGcsKeyDeletingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListGcsKeyDeletingTasksRequestBody,
  ): Promise<ListGcsKeyDeletingTasksResponse> {
    let rows = await listPendingGcsKeyDeletingTasks(this.database, {
      gcsKeyDeletingTaskExecutionTimeMsLe: this.getNow(),
    });
    return {
      tasks: rows.map((row) => ({
        key: row.gcsKeyDeletingTaskKey,
      })),
    };
  }
}
