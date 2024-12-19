import { SPANNER_DATABASE } from "../common/spanner_database";
import { listSubtitleFormattingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListSubtitleFormattingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListSubtitleFormattingTasksRequestBody,
  ListSubtitleFormattingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListSubtitleFormattingTasksHandler extends ListSubtitleFormattingTasksHandlerInterface {
  public static create(): ListSubtitleFormattingTasksHandler {
    return new ListSubtitleFormattingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListSubtitleFormattingTasksRequestBody,
  ): Promise<ListSubtitleFormattingTasksResponse> {
    let rows = await listSubtitleFormattingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        containerId: row.subtitleFormattingTaskContainerId,
        gcsFilename: row.subtitleFormattingTaskGcsFilename,
      })),
    };
  }
}
