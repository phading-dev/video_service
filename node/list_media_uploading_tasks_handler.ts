import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingMediaUploadingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListMediaUploadingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListMediaUploadingTasksRequestBody,
  ListMediaUploadingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListMediaUploadingTasksHandler extends ListMediaUploadingTasksHandlerInterface {
  public static create(): ListMediaUploadingTasksHandler {
    return new ListMediaUploadingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListMediaUploadingTasksRequestBody,
  ): Promise<ListMediaUploadingTasksResponse> {
    let rows = await listPendingMediaUploadingTasks(this.database, {
      mediaUploadingTaskExecutionTimeMsLe: this.getNow(),
    });
    return {
      tasks: rows.map((row) => ({
        containerId: row.mediaUploadingTaskContainerId,
        gcsDirname: row.mediaUploadingTaskGcsDirname,
      })),
    };
  }
}
