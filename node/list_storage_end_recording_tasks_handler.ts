import { SPANNER_DATABASE } from "../common/spanner_database";
import { listStorageEndRecordingTasks } from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { ListStorageEndRecordingTasksHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  ListStorageEndRecordingTasksRequestBody,
  ListStorageEndRecordingTasksResponse,
} from "@phading/video_service_interface/node/interface";

export class ListStorageEndRecordingTasksHandler extends ListStorageEndRecordingTasksHandlerInterface {
  public static create(): ListStorageEndRecordingTasksHandler {
    return new ListStorageEndRecordingTasksHandler(SPANNER_DATABASE, () =>
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
    body: ListStorageEndRecordingTasksRequestBody,
  ): Promise<ListStorageEndRecordingTasksResponse> {
    let rows = await listStorageEndRecordingTasks(this.database, this.getNow());
    return {
      tasks: rows.map((row) => ({
        r2Dirname: row.storageEndRecordingTaskPayload.r2Dirname,
        accountId: row.storageEndRecordingTaskPayload.accountId,
        endTimeMs: row.storageEndRecordingTaskPayload.endTimeMs,
      })),
    };
  }
}
