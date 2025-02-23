import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingUploadedRecordingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListUploadedRecordingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListUploadedRecordingTasksRequestBody,
  ListUploadedRecordingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListUploadedRecordingTasksHandler extends ListUploadedRecordingTasksHandlerInterface {
  public static create(): ListUploadedRecordingTasksHandler {
    return new ListUploadedRecordingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListUploadedRecordingTasksRequestBody,
  ): Promise<ListUploadedRecordingTasksResponse> {
    let rows = await listPendingUploadedRecordingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        gcsFilename: row.uploadedRecordingTaskGcsFilename,
        accountId: row.uploadedRecordingTaskPayload.accountId,
        totalBytes: row.uploadedRecordingTaskPayload.totalBytes,
      })),
    };
  }
}
