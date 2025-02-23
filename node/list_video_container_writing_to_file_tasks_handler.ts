import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingVideoContainerWritingToFileTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListVideoContainerWritingToFileTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListVideoContainerWritingToFileTasksRequestBody,
  ListVideoContainerWritingToFileTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListVideoContainerWritingToFileTasksHandler extends ListVideoContainerWritingToFileTasksHandlerInterface {
  public static create(): ListVideoContainerWritingToFileTasksHandler {
    return new ListVideoContainerWritingToFileTasksHandler(
      SPANNER_DATABASE,
      () => Date.now(),
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
    body: ListVideoContainerWritingToFileTasksRequestBody,
  ): Promise<ListVideoContainerWritingToFileTasksResponse> {
    let rows = await listPendingVideoContainerWritingToFileTasks(
      this.database,
      this.getNow(),
    );
    return {
      tasks: rows.map((row) => ({
        containerId: row.videoContainerWritingToFileTaskContainerId,
        version: row.videoContainerWritingToFileTaskVersion,
      })),
    };
  }
}
