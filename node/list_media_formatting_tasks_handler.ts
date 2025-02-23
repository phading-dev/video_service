import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingMediaFormattingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListMediaFormattingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListMediaFormattingTasksRequestBody,
  ListMediaFormattingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListMediaFormattingTasksHandler extends ListMediaFormattingTasksHandlerInterface {
  public static create(): ListMediaFormattingTasksHandler {
    return new ListMediaFormattingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListMediaFormattingTasksRequestBody,
  ): Promise<ListMediaFormattingTasksResponse> {
    let rows = await listPendingMediaFormattingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        containerId: row.mediaFormattingTaskContainerId,
        gcsFilename: row.mediaFormattingTaskGcsFilename,
      })),
    };
  }
}
