import { SPANNER_DATABASE } from "../common/spanner_database";
import { listPendingStorageStartRecordingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListStorageStartRecordingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListStorageStartRecordingTasksRequestBody,
  ListStorageStartRecordingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListStorageStartRecordingTasksHandler extends ListStorageStartRecordingTasksHandlerInterface {
  public static create(): ListStorageStartRecordingTasksHandler {
    return new ListStorageStartRecordingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListStorageStartRecordingTasksRequestBody,
  ): Promise<ListStorageStartRecordingTasksResponse> {
    let rows = await listPendingStorageStartRecordingTasks(
      this.database,
      this.getNow(),
    );
    return {
      tasks: rows.map((row) => ({
        r2Dirname: row.storageStartRecordingTaskR2Dirname,
        accountId: row.storageStartRecordingTaskPayload.accountId,
        totalBytes: row.storageStartRecordingTaskPayload.totalBytes,
        startTimeMs: row.storageStartRecordingTaskPayload.startTimeMs,
      })),
    };
  }
}
