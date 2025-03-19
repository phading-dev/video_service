import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingVideoContainerSyncingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListVideoContainerSyncingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListVideoContainerSyncingTasksRequestBody,
  ListVideoContainerSyncingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListVideoContainerSyncingTasksHandler extends ListVideoContainerSyncingTasksHandlerInterface {
  public static create(): ListVideoContainerSyncingTasksHandler {
    return new ListVideoContainerSyncingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListVideoContainerSyncingTasksRequestBody,
  ): Promise<ListVideoContainerSyncingTasksResponse> {
    let rows = await listPendingVideoContainerSyncingTasks(this.database, {
      videoContainerSyncingTaskExecutionTimeMsLe: this.getNow(),
    });
    return {
      tasks: rows.map((row) => ({
        containerId: row.videoContainerSyncingTaskContainerId,
        version: row.videoContainerSyncingTaskVersion,
      })),
    };
  }
}
