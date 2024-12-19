import { VideoContainerData, VIDEO_CONTAINER_DATA, GcsFileDeleteTaskPayload, GCS_FILE_DELETE_TASK_PAYLOAD } from './schema';
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

export interface ListGcsFileDeleteTasksRow {
  gcsFileDeleteTaskFilename: string,
  gcsFileDeleteTaskPayload: GcsFileDeleteTaskPayload,
  gcsFileDeleteTaskExecutionTimestamp: number,
}

export let LIST_GCS_FILE_DELETE_TASKS_ROW: MessageDescriptor<ListGcsFileDeleteTasksRow> = {
  name: 'ListGcsFileDeleteTasksRow',
  fields: [{
    name: 'gcsFileDeleteTaskFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeleteTaskPayload',
    index: 2,
    messageType: GCS_FILE_DELETE_TASK_PAYLOAD,
  }, {
    name: 'gcsFileDeleteTaskExecutionTimestamp',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listGcsFileDeleteTasks(
  runner: Database | Transaction,
  gcsFileDeleteTaskExecutionTimestampLt: number,
): Promise<Array<ListGcsFileDeleteTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeleteTask.filename, GcsFileDeleteTask.payload, GcsFileDeleteTask.executionTimestamp FROM GcsFileDeleteTask WHERE GcsFileDeleteTask.executionTimestamp < @gcsFileDeleteTaskExecutionTimestampLt ORDER BY GcsFileDeleteTask.executionTimestamp",
    params: {
      gcsFileDeleteTaskExecutionTimestampLt: new Date(gcsFileDeleteTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      gcsFileDeleteTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListGcsFileDeleteTasksRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeleteTaskFilename: row.at(0).value,
      gcsFileDeleteTaskPayload: deserializeMessage(row.at(1).value, GCS_FILE_DELETE_TASK_PAYLOAD),
      gcsFileDeleteTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListR2KeyDeleteTasksRow {
  r2KeyDeleteTaskKey: string,
  r2KeyDeleteTaskExecutionTimestamp: number,
}

export let LIST_R2_KEY_DELETE_TASKS_ROW: MessageDescriptor<ListR2KeyDeleteTasksRow> = {
  name: 'ListR2KeyDeleteTasksRow',
  fields: [{
    name: 'r2KeyDeleteTaskKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2KeyDeleteTaskExecutionTimestamp',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listR2KeyDeleteTasks(
  runner: Database | Transaction,
  r2KeyDeleteTaskExecutionTimestampLt: number,
): Promise<Array<ListR2KeyDeleteTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeleteTask.key, R2KeyDeleteTask.executionTimestamp FROM R2KeyDeleteTask WHERE R2KeyDeleteTask.executionTimestamp < @r2KeyDeleteTaskExecutionTimestampLt ORDER BY R2KeyDeleteTask.executionTimestamp",
    params: {
      r2KeyDeleteTaskExecutionTimestampLt: new Date(r2KeyDeleteTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      r2KeyDeleteTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListR2KeyDeleteTasksRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeleteTaskKey: row.at(0).value,
      r2KeyDeleteTaskExecutionTimestamp: row.at(1).value.valueOf(),
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

export function insertGcsFileDeleteTaskStatement(
  filename: string,
  payload: GcsFileDeleteTaskPayload,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT GcsFileDeleteTask (filename, payload, executionTimestamp, createdTimestamp) VALUES (@filename, @payload, @executionTimestamp, @createdTimestamp)",
    params: {
      filename: filename,
      payload: Buffer.from(serializeMessage(payload, GCS_FILE_DELETE_TASK_PAYLOAD).buffer),
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

export function insertR2KeyDeleteTaskStatement(
  key: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT R2KeyDeleteTask (key, executionTimestamp, createdTimestamp) VALUES (@key, @executionTimestamp, @createdTimestamp)",
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

export function updateGcsFileDeleteTaskStatement(
  gcsFileDeleteTaskFilenameEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE GcsFileDeleteTask SET executionTimestamp = @setExecutionTimestamp WHERE GcsFileDeleteTask.filename = @gcsFileDeleteTaskFilenameEq",
    params: {
      gcsFileDeleteTaskFilenameEq: gcsFileDeleteTaskFilenameEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      gcsFileDeleteTaskFilenameEq: { type: "string" },
      setExecutionTimestamp: { type: "timestamp" },
    }
  };
}

export function updateR2KeyDeleteTaskStatement(
  r2KeyDeleteTaskKeyEq: string,
  setExecutionTimestamp: number,
): Statement {
  return {
    sql: "UPDATE R2KeyDeleteTask SET executionTimestamp = @setExecutionTimestamp WHERE R2KeyDeleteTask.key = @r2KeyDeleteTaskKeyEq",
    params: {
      r2KeyDeleteTaskKeyEq: r2KeyDeleteTaskKeyEq,
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
    },
    types: {
      r2KeyDeleteTaskKeyEq: { type: "string" },
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

export function deleteGcsFileDeleteTaskStatement(
  gcsFileDeleteTaskFilenameEq: string,
): Statement {
  return {
    sql: "DELETE GcsFileDeleteTask WHERE GcsFileDeleteTask.filename = @gcsFileDeleteTaskFilenameEq",
    params: {
      gcsFileDeleteTaskFilenameEq: gcsFileDeleteTaskFilenameEq,
    },
    types: {
      gcsFileDeleteTaskFilenameEq: { type: "string" },
    }
  };
}

export function deleteR2KeyDeleteTaskStatement(
  r2KeyDeleteTaskKeyEq: string,
): Statement {
  return {
    sql: "DELETE R2KeyDeleteTask WHERE R2KeyDeleteTask.key = @r2KeyDeleteTaskKeyEq",
    params: {
      r2KeyDeleteTaskKeyEq: r2KeyDeleteTaskKeyEq,
    },
    types: {
      r2KeyDeleteTaskKeyEq: { type: "string" },
    }
  };
}
