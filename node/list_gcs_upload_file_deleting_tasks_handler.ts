import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingGcsUploadFileDeletingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListGcsUploadFileDeletingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListGcsUploadFileDeletingTasksRequestBody,
  ListGcsUploadFileDeletingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListGcsUploadFileDeletingTasksHandler extends ListGcsUploadFileDeletingTasksHandlerInterface {
  public static create(): ListGcsUploadFileDeletingTasksHandler {
    return new ListGcsUploadFileDeletingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListGcsUploadFileDeletingTasksRequestBody,
  ): Promise<ListGcsUploadFileDeletingTasksResponse> {
    let rows = await listPendingGcsUploadFileDeletingTasks(this.database, {
      gcsUploadFileDeletingTaskExecutionTimeMsLe: this.getNow(),
    });
    return {
      tasks: rows.map((row) => ({
        gcsFilename: row.gcsUploadFileDeletingTaskFilename,
        uploadSessionUrl: row.gcsUploadFileDeletingTaskUploadSessionUrl,
      })),
    };
  }
}
