import { VideoContainerData, VIDEO_CONTAINER_DATA, GcsFileDeletingTaskPayload, GCS_FILE_DELETING_TASK_PAYLOAD } from './schema';
import { deserializeMessage, serializeMessage } from '@selfage/message/serializer';
import { Database, Transaction, Spanner } from '@google-cloud/spanner';
import { MessageDescriptor, PrimitiveType } from '@selfage/message/descriptor';
import { Statement } from '@google-cloud/spanner/build/src/transaction';

export interface GetVideoContainerRow {
  videoContainerData: VideoContainerData,
}

export let GET_VIDEO_CONTAINER_ROW: MessageDescriptor<GetVideoContainerRow> = {
  name: 'GetVideoContainerRow',
  fields: [{
    name: 'videoContainerData',
    index: 1,
    messageType: VIDEO_CONTAINER_DATA,
  }],
};

export async function getVideoContainer(
  runner: Database | Transaction,
  videoContainerContainerIdEq: string,
): Promise<Array<GetVideoContainerRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainer.data FROM VideoContainer WHERE VideoContainer.containerId = @videoContainerContainerIdEq",
    params: {
      videoContainerContainerIdEq: videoContainerContainerIdEq,
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetVideoContainerRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerData: deserializeMessage(row.at(0).value, VIDEO_CONTAINER_DATA),
    });
  }
  return resRows;
}

export interface CheckGcsFileRow {
  gcsFileFilename: string,
}

export let CHECK_GCS_FILE_ROW: MessageDescriptor<CheckGcsFileRow> = {
  name: 'CheckGcsFileRow',
  fields: [{
    name: 'gcsFileFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function checkGcsFile(
  runner: Database | Transaction,
  gcsFileFilenameEq: string,
): Promise<Array<CheckGcsFileRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFile.filename FROM GcsFile WHERE GcsFile.filename = @gcsFileFilenameEq",
    params: {
      gcsFileFilenameEq: gcsFileFilenameEq,
    },
    types: {
      gcsFileFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<CheckGcsFileRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileFilename: row.at(0).value,
    });
  }
  return resRows;
}

export interface CheckR2KeyRow {
  r2KeyKey: string,
}

export let CHECK_R2_KEY_ROW: MessageDescriptor<CheckR2KeyRow> = {
  name: 'CheckR2KeyRow',
  fields: [{
    name: 'r2KeyKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function checkR2Key(
  runner: Database | Transaction,
  r2KeyKeyEq: string,
): Promise<Array<CheckR2KeyRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2Key.key FROM R2Key WHERE R2Key.key = @r2KeyKeyEq",
    params: {
      r2KeyKeyEq: r2KeyKeyEq,
    },
    types: {
      r2KeyKeyEq: { type: "string" },
    }
  });
  let resRows = new Array<CheckR2KeyRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyKey: row.at(0).value,
    });
  }
  return resRows;
}

export interface ListVideoContainerWritingToFileTasksRow {
  videoContainerWritingToFileTaskContainerId: string,
  videoContainerWritingToFileTaskVersion: number,
  videoContainerWritingToFileTaskExecutionTimestamp: number,
}

export let LIST_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW: MessageDescriptor<ListVideoContainerWritingToFileTasksRow> = {
  name: 'ListVideoContainerWritingToFileTasksRow',
  fields: [{
    name: 'videoContainerWritingToFileTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerWritingToFileTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerWritingToFileTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listVideoContainerWritingToFileTasks(
  runner: Database | Transaction,
  videoContainerWritingToFileTaskExecutionTimestampLt: number,
): Promise<Array<ListVideoContainerWritingToFileTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerWritingToFileTask.containerId, VideoContainerWritingToFileTask.version, VideoContainerWritingToFileTask.executionTimestamp FROM VideoContainerWritingToFileTask WHERE VideoContainerWritingToFileTask.executionTimestamp < @videoContainerWritingToFileTaskExecutionTimestampLt ORDER BY VideoContainerWritingToFileTask.executionTimestamp",
    params: {
      videoContainerWritingToFileTaskExecutionTimestampLt: new Date(videoContainerWritingToFileTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListVideoContainerWritingToFileTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerWritingToFileTaskContainerId: row.at(0).value,
      videoContainerWritingToFileTaskVersion: row.at(1).value.value,
      videoContainerWritingToFileTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListVideoContainerSyncingTasksRow {
  videoContainerSyncingTaskContainerId: string,
  videoContainerSyncingTaskVersion: number,
  videoContainerSyncingTaskExecutionTimestamp: number,
}

export let LIST_VIDEO_CONTAINER_SYNCING_TASKS_ROW: MessageDescriptor<ListVideoContainerSyncingTasksRow> = {
  name: 'ListVideoContainerSyncingTasksRow',
  fields: [{
    name: 'videoContainerSyncingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerSyncingTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerSyncingTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listVideoContainerSyncingTasks(
  runner: Database | Transaction,
  videoContainerSyncingTaskExecutionTimestampLt: number,
): Promise<Array<ListVideoContainerSyncingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.containerId, VideoContainerSyncingTask.version, VideoContainerSyncingTask.executionTimestamp FROM VideoContainerSyncingTask WHERE VideoContainerSyncingTask.executionTimestamp < @videoContainerSyncingTaskExecutionTimestampLt ORDER BY VideoContainerSyncingTask.executionTimestamp",
    params: {
      videoContainerSyncingTaskExecutionTimestampLt: new Date(videoContainerSyncingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      videoContainerSyncingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListVideoContainerSyncingTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskContainerId: row.at(0).value,
      videoContainerSyncingTaskVersion: row.at(1).value.value,
      videoContainerSyncingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListMediaFormattingTasksRow {
  mediaFormattingTaskContainerId: string,
  mediaFormattingTaskGcsFilename: string,
  mediaFormattingTaskExecutionTimestamp: number,
}

export let LIST_MEDIA_FORMATTING_TASKS_ROW: MessageDescriptor<ListMediaFormattingTasksRow> = {
  name: 'ListMediaFormattingTasksRow',
  fields: [{
    name: 'mediaFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'mediaFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'mediaFormattingTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listMediaFormattingTasks(
  runner: Database | Transaction,
  mediaFormattingTaskExecutionTimestampLt: number,
): Promise<Array<ListMediaFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.containerId, MediaFormattingTask.gcsFilename, MediaFormattingTask.executionTimestamp FROM MediaFormattingTask WHERE MediaFormattingTask.executionTimestamp < @mediaFormattingTaskExecutionTimestampLt ORDER BY MediaFormattingTask.executionTimestamp",
    params: {
      mediaFormattingTaskExecutionTimestampLt: new Date(mediaFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      mediaFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListMediaFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskContainerId: row.at(0).value,
      mediaFormattingTaskGcsFilename: row.at(1).value,
      mediaFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListSubtitleFormattingTasksRow {
  subtitleFormattingTaskContainerId: string,
  subtitleFormattingTaskGcsFilename: string,
  subtitleFormattingTaskExecutionTimestamp: number,
}

export let LIST_SUBTITLE_FORMATTING_TASKS_ROW: MessageDescriptor<ListSubtitleFormattingTasksRow> = {
  name: 'ListSubtitleFormattingTasksRow',
  fields: [{
    name: 'subtitleFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'subtitleFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'subtitleFormattingTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listSubtitleFormattingTasks(
  runner: Database | Transaction,
  subtitleFormattingTaskExecutionTimestampLt: number,
): Promise<Array<ListSubtitleFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.gcsFilename, SubtitleFormattingTask.executionTimestamp FROM SubtitleFormattingTask WHERE SubtitleFormattingTask.executionTimestamp < @subtitleFormattingTaskExecutionTimestampLt ORDER BY SubtitleFormattingTask.executionTimestamp",
    params: {
      subtitleFormattingTaskExecutionTimestampLt: new Date(subtitleFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      subtitleFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListSubtitleFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskContainerId: row.at(0).value,
      subtitleFormattingTaskGcsFilename: row.at(1).value,
      subtitleFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListGcsFileDeletingTasksRow {
  gcsFileDeletingTaskFilename: string,
  gcsFileDeletingTaskPayload: GcsFileDeletingTaskPayload,
  gcsFileDeletingTaskExecutionTimestamp: number,
}

export let LIST_GCS_FILE_DELETING_TASKS_ROW: MessageDescriptor<ListGcsFileDeletingTasksRow> = {
  name: 'ListGcsFileDeletingTasksRow',
  fields: [{
    name: 'gcsFileDeletingTaskFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskPayload',
    index: 2,
    messageType: GCS_FILE_DELETING_TASK_PAYLOAD,
  }, {
    name: 'gcsFileDeletingTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listGcsFileDeletingTasks(
  runner: Database | Transaction,
  gcsFileDeletingTaskExecutionTimestampLt: number,
): Promise<Array<ListGcsFileDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeletingTask.filename, GcsFileDeletingTask.payload, GcsFileDeletingTask.executionTimestamp FROM GcsFileDeletingTask WHERE GcsFileDeletingTask.executionTimestamp < @gcsFileDeletingTaskExecutionTimestampLt ORDER BY GcsFileDeletingTask.executionTimestamp",
    params: {
      gcsFileDeletingTaskExecutionTimestampLt: new Date(gcsFileDeletingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      gcsFileDeletingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListGcsFileDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeletingTaskFilename: row.at(0).value,
      gcsFileDeletingTaskPayload: deserializeMessage(row.at(1).value, GCS_FILE_DELETING_TASK_PAYLOAD),
      gcsFileDeletingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListR2KeyDeletingTasksRow {
  r2KeyDeletingTaskKey: string,
  r2KeyDeletingTaskExecutionTimestamp: number,
}

export let LIST_R2_KEY_DELETING_TASKS_ROW: MessageDescriptor<ListR2KeyDeletingTasksRow> = {
  name: 'ListR2KeyDeletingTasksRow',
  fields: [{
    name: 'r2KeyDeletingTaskKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2KeyDeletingTaskExecutionTimestamp',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listR2KeyDeletingTasks(
  runner: Database | Transaction,
  r2KeyDeletingTaskExecutionTimestampLt: number,
): Promise<Array<ListR2KeyDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeletingTask.key, R2KeyDeletingTask.executionTimestamp FROM R2KeyDeletingTask WHERE R2KeyDeletingTask.executionTimestamp < @r2KeyDeletingTaskExecutionTimestampLt ORDER BY R2KeyDeletingTask.executionTimestamp",
    params: {
      r2KeyDeletingTaskExecutionTimestampLt: new Date(r2KeyDeletingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      r2KeyDeletingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListR2KeyDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeletingTaskKey: row.at(0).value,
      r2KeyDeletingTaskExecutionTimestamp: row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function insertVideoContainerStatement(
  containerId: string,
  data: VideoContainerData,
): Statement {
  return {
    sql: "INSERT VideoContainer (containerId, data) VALUES (@containerId, @data)",
    params: {
      containerId: containerId,
      data: Buffer.from(serializeMessage(data, VIDEO_CONTAINER_DATA).buffer),
    },
    types: {
      containerId: { type: "string" },
      data: { type: "bytes" },
    }
  };
}

export function insertGcsFileStatement(
  filename: string,
): Statement {
  return {
    sql: "INSERT GcsFile (filename) VALUES (@filename)",
    params: {
      filename: filename,
    },
    types: {
      filename: { type: "string" },
    }
  };
}

export function insertR2KeyStatement(
  key: string,
): Statement {
  return {
    sql: "INSERT R2Key (key) VALUES (@key)",
    params: {
      key: key,
    },
    types: {
      key: { type: "string" },
    }
  };
}

export function insertVideoContainerWritingToFileTaskStatement(
  containerId: string,
  version: number,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT VideoContainerWritingToFileTask (containerId, version, executionTimestamp, createdTimestamp) VALUES (@containerId, @version, @executionTimestamp, @createdTimestamp)",
    params: {
      containerId: containerId,
      version: Spanner.float(version),
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertVideoContainerSyncingTaskStatement(
  containerId: string,
  version: number,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT VideoContainerSyncingTask (containerId, version, executionTimestamp, createdTimestamp) VALUES (@containerId, @version, @executionTimestamp, @createdTimestamp)",
    params: {
      containerId: containerId,
      version: Spanner.float(version),
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertMediaFormattingTaskStatement(
  containerId: string,
  gcsFilename: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT MediaFormattingTask (containerId, gcsFilename, executionTimestamp, createdTimestamp) VALUES (@containerId, @gcsFilename, @executionTimestamp, @createdTimestamp)",
    params: {
      containerId: containerId,
      gcsFilename: gcsFilename,
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertSubtitleFormattingTaskStatement(
  containerId: string,
  gcsFilename: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT SubtitleFormattingTask (containerId, gcsFilename, executionTimestamp, createdTimestamp) VALUES (@containerId, @gcsFilename, @executionTimestamp, @createdTimestamp)",
    params: {
      containerId: containerId,
      gcsFilename: gcsFilename,
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertGcsFileDeletingTaskStatement(
  filename: string,
  payload: GcsFileDeletingTaskPayload,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT GcsFileDeletingTask (filename, payload, executionTimestamp, createdTimestamp) VALUES (@filename, @payload, @executionTimestamp, @createdTimestamp)",
    params: {
      filename: filename,
      payload: Buffer.from(serializeMessage(payload, GCS_FILE_DELETING_TASK_PAYLOAD).buffer),
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      filename: { type: "string" },
      payload: { type: "bytes" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertR2KeyDeletingTaskStatement(
  key: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT R2KeyDeletingTask (key, executionTimestamp, createdTimestamp) VALUES (@key, @executionTimestamp, @createdTimestamp)",
    params: {
      key: key,
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      key: { type: "string" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function updateVideoContainerStatement(
  videoContainerContainerIdEq: string,
  setData: VideoContainerData,
): Statement {
  return {
    sql: "UPDATE VideoContainer SET data = @setData WHERE VideoContainer.containerId = @videoContainerContainerIdEq",
    params: {
      videoContainerContainerIdEq: videoContainerContainerIdEq,
      setData: Buffer.from(serializeMessage(setData, VIDEO_CONTAINER_DATA).buffer),
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
      setData: { type: "bytes" },
    }
  };
}

export function updateVideoContainerWritingToFileTaskStatement(
  videoContainerWritingToFileTaskContainerIdEq: string,
  videoContainerWritingToFileTaskVersionEq: number,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE VideoContainerWritingToFileTask SET executionTimestamp = @setExecutionTimestamp WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(videoContainerWritingToFileTaskVersionEq),
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateVideoContainerSyncingTaskStatement(
  videoContainerSyncingTaskContainerIdEq: string,
  videoContainerSyncingTaskVersionEq: number,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE VideoContainerSyncingTask SET executionTimestamp = @setExecutionTimestamp WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(videoContainerSyncingTaskVersionEq),
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateMediaFormattingTaskStatement(
  mediaFormattingTaskContainerIdEq: string,
  mediaFormattingTaskGcsFilenameEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE MediaFormattingTask SET executionTimestamp = @setExecutionTimestamp WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: mediaFormattingTaskGcsFilenameEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateSubtitleFormattingTaskStatement(
  subtitleFormattingTaskContainerIdEq: string,
  subtitleFormattingTaskGcsFilenameEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE SubtitleFormattingTask SET executionTimestamp = @setExecutionTimestamp WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: subtitleFormattingTaskGcsFilenameEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateGcsFileDeletingTaskStatement(
  gcsFileDeletingTaskFilenameEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE GcsFileDeletingTask SET executionTimestamp = @setExecutionTimestamp WHERE GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq",
    params: {
      gcsFileDeletingTaskFilenameEq: gcsFileDeletingTaskFilenameEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateR2KeyDeletingTaskStatement(
  r2KeyDeletingTaskKeyEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE R2KeyDeletingTask SET executionTimestamp = @setExecutionTimestamp WHERE R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq",
    params: {
      r2KeyDeletingTaskKeyEq: r2KeyDeletingTaskKeyEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function deleteVideoContainerStatement(
  videoContainerContainerIdEq: string,
): Statement {
  return {
    sql: "DELETE VideoContainer WHERE VideoContainer.containerId = @videoContainerContainerIdEq",
    params: {
      videoContainerContainerIdEq: videoContainerContainerIdEq,
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
    }
  };
}

export function deleteGcsFileStatement(
  gcsFileFilenameEq: string,
): Statement {
  return {
    sql: "DELETE GcsFile WHERE GcsFile.filename = @gcsFileFilenameEq",
    params: {
      gcsFileFilenameEq: gcsFileFilenameEq,
    },
    types: {
      gcsFileFilenameEq: { type: "string" },
    }
  };
}

export function deleteR2KeyStatement(
  r2KeyKeyEq: string,
): Statement {
  return {
    sql: "DELETE R2Key WHERE R2Key.key = @r2KeyKeyEq",
    params: {
      r2KeyKeyEq: r2KeyKeyEq,
    },
    types: {
      r2KeyKeyEq: { type: "string" },
    }
  };
}

export function deleteVideoContainerWritingToFileTaskStatement(
  videoContainerWritingToFileTaskContainerIdEq: string,
  videoContainerWritingToFileTaskVersionEq: number,
): Statement {
  return {
    sql: "DELETE VideoContainerWritingToFileTask WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(videoContainerWritingToFileTaskVersionEq),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
    }
  };
}

export function deleteVideoContainerSyncingTaskStatement(
  videoContainerSyncingTaskContainerIdEq: string,
  videoContainerSyncingTaskVersionEq: number,
): Statement {
  return {
    sql: "DELETE VideoContainerSyncingTask WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(videoContainerSyncingTaskVersionEq),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
    }
  };
}

export function deleteMediaFormattingTaskStatement(
  mediaFormattingTaskContainerIdEq: string,
  mediaFormattingTaskGcsFilenameEq: string,
): Statement {
  return {
    sql: "DELETE MediaFormattingTask WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: mediaFormattingTaskGcsFilenameEq,
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export function deleteSubtitleFormattingTaskStatement(
  subtitleFormattingTaskContainerIdEq: string,
  subtitleFormattingTaskGcsFilenameEq: string,
): Statement {
  return {
    sql: "DELETE SubtitleFormattingTask WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: subtitleFormattingTaskGcsFilenameEq,
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export function deleteGcsFileDeletingTaskStatement(
  gcsFileDeletingTaskFilenameEq: string,
): Statement {
  return {
    sql: "DELETE GcsFileDeletingTask WHERE GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq",
    params: {
      gcsFileDeletingTaskFilenameEq: gcsFileDeletingTaskFilenameEq,
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
    }
  };
}

export function deleteR2KeyDeletingTaskStatement(
  r2KeyDeletingTaskKeyEq: string,
): Statement {
  return {
    sql: "DELETE R2KeyDeletingTask WHERE R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq",
    params: {
      r2KeyDeletingTaskKeyEq: r2KeyDeletingTaskKeyEq,
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
    }
  };
}
