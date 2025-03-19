import { VideoContainer, VIDEO_CONTAINER, UploadedRecordingTaskPayload, UPLOADED_RECORDING_TASK_PAYLOAD, StorageStartRecordingTaskPayload, STORAGE_START_RECORDING_TASK_PAYLOAD, StorageEndRecordingTaskPayload, STORAGE_END_RECORDING_TASK_PAYLOAD } from './schema';
import { serializeMessage, deserializeMessage } from '@selfage/message/serializer';
import { Spanner, Database, Transaction } from '@google-cloud/spanner';
import { Statement } from '@google-cloud/spanner/build/src/transaction';
import { PrimitiveType, MessageDescriptor } from '@selfage/message/descriptor';

export function insertVideoContainerStatement(
  args: {
    containerId: string,
    seasonId?: string,
    episodeId?: string,
    accountId?: string,
    data?: VideoContainer,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT VideoContainer (containerId, seasonId, episodeId, accountId, data, createdTimeMs) VALUES (@containerId, @seasonId, @episodeId, @accountId, @data, @createdTimeMs)",
    params: {
      containerId: args.containerId,
      seasonId: args.seasonId == null ? null : args.seasonId,
      episodeId: args.episodeId == null ? null : args.episodeId,
      accountId: args.accountId == null ? null : args.accountId,
      data: args.data == null ? null : Buffer.from(serializeMessage(args.data, VIDEO_CONTAINER).buffer),
      createdTimeMs: args.createdTimeMs == null ? null : Spanner.float(args.createdTimeMs),
    },
    types: {
      containerId: { type: "string" },
      seasonId: { type: "string" },
      episodeId: { type: "string" },
      accountId: { type: "string" },
      data: { type: "bytes" },
      createdTimeMs: { type: "float64" },
    }
  };
}

export function deleteVideoContainerStatement(
  args: {
    videoContainerContainerIdEq: string,
  }
): Statement {
  return {
    sql: "DELETE VideoContainer WHERE (VideoContainer.containerId = @videoContainerContainerIdEq)",
    params: {
      videoContainerContainerIdEq: args.videoContainerContainerIdEq,
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
    }
  };
}

export interface GetVideoContainerRow {
  videoContainerContainerId?: string,
  videoContainerSeasonId?: string,
  videoContainerEpisodeId?: string,
  videoContainerAccountId?: string,
  videoContainerData?: VideoContainer,
  videoContainerCreatedTimeMs?: number,
}

export let GET_VIDEO_CONTAINER_ROW: MessageDescriptor<GetVideoContainerRow> = {
  name: 'GetVideoContainerRow',
  fields: [{
    name: 'videoContainerContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerSeasonId',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerEpisodeId',
    index: 3,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerAccountId',
    index: 4,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerData',
    index: 5,
    messageType: VIDEO_CONTAINER,
  }, {
    name: 'videoContainerCreatedTimeMs',
    index: 6,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getVideoContainer(
  runner: Database | Transaction,
  args: {
    videoContainerContainerIdEq: string,
  }
): Promise<Array<GetVideoContainerRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainer.containerId, VideoContainer.seasonId, VideoContainer.episodeId, VideoContainer.accountId, VideoContainer.data, VideoContainer.createdTimeMs FROM VideoContainer WHERE (VideoContainer.containerId = @videoContainerContainerIdEq)",
    params: {
      videoContainerContainerIdEq: args.videoContainerContainerIdEq,
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetVideoContainerRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      videoContainerSeasonId: row.at(1).value == null ? undefined : row.at(1).value,
      videoContainerEpisodeId: row.at(2).value == null ? undefined : row.at(2).value,
      videoContainerAccountId: row.at(3).value == null ? undefined : row.at(3).value,
      videoContainerData: row.at(4).value == null ? undefined : deserializeMessage(row.at(4).value, VIDEO_CONTAINER),
      videoContainerCreatedTimeMs: row.at(5).value == null ? undefined : row.at(5).value.value,
    });
  }
  return resRows;
}

export function insertGcsFileStatement(
  args: {
    filename: string,
  }
): Statement {
  return {
    sql: "INSERT GcsFile (filename) VALUES (@filename)",
    params: {
      filename: args.filename,
    },
    types: {
      filename: { type: "string" },
    }
  };
}

export function deleteGcsFileStatement(
  args: {
    gcsFileFilenameEq: string,
  }
): Statement {
  return {
    sql: "DELETE GcsFile WHERE (GcsFile.filename = @gcsFileFilenameEq)",
    params: {
      gcsFileFilenameEq: args.gcsFileFilenameEq,
    },
    types: {
      gcsFileFilenameEq: { type: "string" },
    }
  };
}

export interface GetGcsFileRow {
  gcsFileFilename?: string,
}

export let GET_GCS_FILE_ROW: MessageDescriptor<GetGcsFileRow> = {
  name: 'GetGcsFileRow',
  fields: [{
    name: 'gcsFileFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function getGcsFile(
  runner: Database | Transaction,
  args: {
    gcsFileFilenameEq: string,
  }
): Promise<Array<GetGcsFileRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFile.filename FROM GcsFile WHERE (GcsFile.filename = @gcsFileFilenameEq)",
    params: {
      gcsFileFilenameEq: args.gcsFileFilenameEq,
    },
    types: {
      gcsFileFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetGcsFileRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileFilename: row.at(0).value == null ? undefined : row.at(0).value,
    });
  }
  return resRows;
}

export function insertR2KeyStatement(
  args: {
    key: string,
  }
): Statement {
  return {
    sql: "INSERT R2Key (key) VALUES (@key)",
    params: {
      key: args.key,
    },
    types: {
      key: { type: "string" },
    }
  };
}

export function deleteR2KeyStatement(
  args: {
    r2KeyKeyEq: string,
  }
): Statement {
  return {
    sql: "DELETE R2Key WHERE (R2Key.key = @r2KeyKeyEq)",
    params: {
      r2KeyKeyEq: args.r2KeyKeyEq,
    },
    types: {
      r2KeyKeyEq: { type: "string" },
    }
  };
}

export interface GetR2KeyRow {
  r2KeyKey?: string,
}

export let GET_R2_KEY_ROW: MessageDescriptor<GetR2KeyRow> = {
  name: 'GetR2KeyRow',
  fields: [{
    name: 'r2KeyKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function getR2Key(
  runner: Database | Transaction,
  args: {
    r2KeyKeyEq: string,
  }
): Promise<Array<GetR2KeyRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2Key.key FROM R2Key WHERE (R2Key.key = @r2KeyKeyEq)",
    params: {
      r2KeyKeyEq: args.r2KeyKeyEq,
    },
    types: {
      r2KeyKeyEq: { type: "string" },
    }
  });
  let resRows = new Array<GetR2KeyRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyKey: row.at(0).value == null ? undefined : row.at(0).value,
    });
  }
  return resRows;
}

export function insertVideoContainerWritingToFileTaskStatement(
  args: {
    containerId: string,
    version: number,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT VideoContainerWritingToFileTask (containerId, version, retryCount, executionTimeMs, createdTimeMs) VALUES (@containerId, @version, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: args.containerId,
      version: Spanner.float(args.version),
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteVideoContainerWritingToFileTaskStatement(
  args: {
    videoContainerWritingToFileTaskContainerIdEq: string,
    videoContainerWritingToFileTaskVersionEq: number,
  }
): Statement {
  return {
    sql: "DELETE VideoContainerWritingToFileTask WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: args.videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(args.videoContainerWritingToFileTaskVersionEq),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
    }
  };
}

export interface GetVideoContainerWritingToFileTaskRow {
  videoContainerWritingToFileTaskContainerId?: string,
  videoContainerWritingToFileTaskVersion?: number,
  videoContainerWritingToFileTaskRetryCount?: number,
  videoContainerWritingToFileTaskExecutionTimeMs?: number,
  videoContainerWritingToFileTaskCreatedTimeMs?: number,
}

export let GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW: MessageDescriptor<GetVideoContainerWritingToFileTaskRow> = {
  name: 'GetVideoContainerWritingToFileTaskRow',
  fields: [{
    name: 'videoContainerWritingToFileTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerWritingToFileTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerWritingToFileTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerWritingToFileTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerWritingToFileTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getVideoContainerWritingToFileTask(
  runner: Database | Transaction,
  args: {
    videoContainerWritingToFileTaskContainerIdEq: string,
    videoContainerWritingToFileTaskVersionEq: number,
  }
): Promise<Array<GetVideoContainerWritingToFileTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerWritingToFileTask.containerId, VideoContainerWritingToFileTask.version, VideoContainerWritingToFileTask.retryCount, VideoContainerWritingToFileTask.executionTimeMs, VideoContainerWritingToFileTask.createdTimeMs FROM VideoContainerWritingToFileTask WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: args.videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(args.videoContainerWritingToFileTaskVersionEq),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
    }
  });
  let resRows = new Array<GetVideoContainerWritingToFileTaskRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerWritingToFileTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      videoContainerWritingToFileTaskVersion: row.at(1).value == null ? undefined : row.at(1).value.value,
      videoContainerWritingToFileTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      videoContainerWritingToFileTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      videoContainerWritingToFileTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingVideoContainerWritingToFileTasksRow {
  videoContainerWritingToFileTaskContainerId?: string,
  videoContainerWritingToFileTaskVersion?: number,
}

export let LIST_PENDING_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW: MessageDescriptor<ListPendingVideoContainerWritingToFileTasksRow> = {
  name: 'ListPendingVideoContainerWritingToFileTasksRow',
  fields: [{
    name: 'videoContainerWritingToFileTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerWritingToFileTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listPendingVideoContainerWritingToFileTasks(
  runner: Database | Transaction,
  args: {
    videoContainerWritingToFileTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingVideoContainerWritingToFileTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerWritingToFileTask.containerId, VideoContainerWritingToFileTask.version FROM VideoContainerWritingToFileTask WHERE VideoContainerWritingToFileTask.executionTimeMs <= @videoContainerWritingToFileTaskExecutionTimeMsLe",
    params: {
      videoContainerWritingToFileTaskExecutionTimeMsLe: args.videoContainerWritingToFileTaskExecutionTimeMsLe == null ? null : new Date(args.videoContainerWritingToFileTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingVideoContainerWritingToFileTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerWritingToFileTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      videoContainerWritingToFileTaskVersion: row.at(1).value == null ? undefined : row.at(1).value.value,
    });
  }
  return resRows;
}

export interface GetVideoContainerWritingToFileTaskMetadataRow {
  videoContainerWritingToFileTaskRetryCount?: number,
  videoContainerWritingToFileTaskExecutionTimeMs?: number,
}

export let GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_METADATA_ROW: MessageDescriptor<GetVideoContainerWritingToFileTaskMetadataRow> = {
  name: 'GetVideoContainerWritingToFileTaskMetadataRow',
  fields: [{
    name: 'videoContainerWritingToFileTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerWritingToFileTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getVideoContainerWritingToFileTaskMetadata(
  runner: Database | Transaction,
  args: {
    videoContainerWritingToFileTaskContainerIdEq: string,
    videoContainerWritingToFileTaskVersionEq: number,
  }
): Promise<Array<GetVideoContainerWritingToFileTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerWritingToFileTask.retryCount, VideoContainerWritingToFileTask.executionTimeMs FROM VideoContainerWritingToFileTask WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: args.videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(args.videoContainerWritingToFileTaskVersionEq),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
    }
  });
  let resRows = new Array<GetVideoContainerWritingToFileTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerWritingToFileTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      videoContainerWritingToFileTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateVideoContainerWritingToFileTaskMetadataStatement(
  args: {
    videoContainerWritingToFileTaskContainerIdEq: string,
    videoContainerWritingToFileTaskVersionEq: number,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE VideoContainerWritingToFileTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: args.videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(args.videoContainerWritingToFileTaskVersionEq),
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertVideoContainerSyncingTaskStatement(
  args: {
    containerId: string,
    version: number,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT VideoContainerSyncingTask (containerId, version, retryCount, executionTimeMs, createdTimeMs) VALUES (@containerId, @version, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: args.containerId,
      version: Spanner.float(args.version),
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteVideoContainerSyncingTaskStatement(
  args: {
    videoContainerSyncingTaskContainerIdEq: string,
    videoContainerSyncingTaskVersionEq: number,
  }
): Statement {
  return {
    sql: "DELETE VideoContainerSyncingTask WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: args.videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(args.videoContainerSyncingTaskVersionEq),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
    }
  };
}

export interface GetVideoContainerSyncingTaskRow {
  videoContainerSyncingTaskContainerId?: string,
  videoContainerSyncingTaskVersion?: number,
  videoContainerSyncingTaskRetryCount?: number,
  videoContainerSyncingTaskExecutionTimeMs?: number,
  videoContainerSyncingTaskCreatedTimeMs?: number,
}

export let GET_VIDEO_CONTAINER_SYNCING_TASK_ROW: MessageDescriptor<GetVideoContainerSyncingTaskRow> = {
  name: 'GetVideoContainerSyncingTaskRow',
  fields: [{
    name: 'videoContainerSyncingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerSyncingTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerSyncingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerSyncingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerSyncingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getVideoContainerSyncingTask(
  runner: Database | Transaction,
  args: {
    videoContainerSyncingTaskContainerIdEq: string,
    videoContainerSyncingTaskVersionEq: number,
  }
): Promise<Array<GetVideoContainerSyncingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.containerId, VideoContainerSyncingTask.version, VideoContainerSyncingTask.retryCount, VideoContainerSyncingTask.executionTimeMs, VideoContainerSyncingTask.createdTimeMs FROM VideoContainerSyncingTask WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: args.videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(args.videoContainerSyncingTaskVersionEq),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
    }
  });
  let resRows = new Array<GetVideoContainerSyncingTaskRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      videoContainerSyncingTaskVersion: row.at(1).value == null ? undefined : row.at(1).value.value,
      videoContainerSyncingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      videoContainerSyncingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      videoContainerSyncingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingVideoContainerSyncingTasksRow {
  videoContainerSyncingTaskContainerId?: string,
  videoContainerSyncingTaskVersion?: number,
}

export let LIST_PENDING_VIDEO_CONTAINER_SYNCING_TASKS_ROW: MessageDescriptor<ListPendingVideoContainerSyncingTasksRow> = {
  name: 'ListPendingVideoContainerSyncingTasksRow',
  fields: [{
    name: 'videoContainerSyncingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'videoContainerSyncingTaskVersion',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listPendingVideoContainerSyncingTasks(
  runner: Database | Transaction,
  args: {
    videoContainerSyncingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingVideoContainerSyncingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.containerId, VideoContainerSyncingTask.version FROM VideoContainerSyncingTask WHERE VideoContainerSyncingTask.executionTimeMs <= @videoContainerSyncingTaskExecutionTimeMsLe",
    params: {
      videoContainerSyncingTaskExecutionTimeMsLe: args.videoContainerSyncingTaskExecutionTimeMsLe == null ? null : new Date(args.videoContainerSyncingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      videoContainerSyncingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingVideoContainerSyncingTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      videoContainerSyncingTaskVersion: row.at(1).value == null ? undefined : row.at(1).value.value,
    });
  }
  return resRows;
}

export interface GetVideoContainerSyncingTaskMetadataRow {
  videoContainerSyncingTaskRetryCount?: number,
  videoContainerSyncingTaskExecutionTimeMs?: number,
}

export let GET_VIDEO_CONTAINER_SYNCING_TASK_METADATA_ROW: MessageDescriptor<GetVideoContainerSyncingTaskMetadataRow> = {
  name: 'GetVideoContainerSyncingTaskMetadataRow',
  fields: [{
    name: 'videoContainerSyncingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'videoContainerSyncingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getVideoContainerSyncingTaskMetadata(
  runner: Database | Transaction,
  args: {
    videoContainerSyncingTaskContainerIdEq: string,
    videoContainerSyncingTaskVersionEq: number,
  }
): Promise<Array<GetVideoContainerSyncingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.retryCount, VideoContainerSyncingTask.executionTimeMs FROM VideoContainerSyncingTask WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: args.videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(args.videoContainerSyncingTaskVersionEq),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
    }
  });
  let resRows = new Array<GetVideoContainerSyncingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      videoContainerSyncingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateVideoContainerSyncingTaskMetadataStatement(
  args: {
    videoContainerSyncingTaskContainerIdEq: string,
    videoContainerSyncingTaskVersionEq: number,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE VideoContainerSyncingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: args.videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(args.videoContainerSyncingTaskVersionEq),
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertUploadedRecordingTaskStatement(
  args: {
    gcsFilename: string,
    payload: UploadedRecordingTaskPayload,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT UploadedRecordingTask (gcsFilename, payload, retryCount, executionTimeMs, createdTimeMs) VALUES (@gcsFilename, @payload, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      gcsFilename: args.gcsFilename,
      payload: Buffer.from(serializeMessage(args.payload, UPLOADED_RECORDING_TASK_PAYLOAD).buffer),
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      gcsFilename: { type: "string" },
      payload: { type: "bytes" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteUploadedRecordingTaskStatement(
  args: {
    uploadedRecordingTaskGcsFilenameEq: string,
  }
): Statement {
  return {
    sql: "DELETE UploadedRecordingTask WHERE (UploadedRecordingTask.gcsFilename = @uploadedRecordingTaskGcsFilenameEq)",
    params: {
      uploadedRecordingTaskGcsFilenameEq: args.uploadedRecordingTaskGcsFilenameEq,
    },
    types: {
      uploadedRecordingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export interface GetUploadedRecordingTaskRow {
  uploadedRecordingTaskGcsFilename?: string,
  uploadedRecordingTaskPayload?: UploadedRecordingTaskPayload,
  uploadedRecordingTaskRetryCount?: number,
  uploadedRecordingTaskExecutionTimeMs?: number,
  uploadedRecordingTaskCreatedTimeMs?: number,
}

export let GET_UPLOADED_RECORDING_TASK_ROW: MessageDescriptor<GetUploadedRecordingTaskRow> = {
  name: 'GetUploadedRecordingTaskRow',
  fields: [{
    name: 'uploadedRecordingTaskGcsFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'uploadedRecordingTaskPayload',
    index: 2,
    messageType: UPLOADED_RECORDING_TASK_PAYLOAD,
  }, {
    name: 'uploadedRecordingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'uploadedRecordingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'uploadedRecordingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getUploadedRecordingTask(
  runner: Database | Transaction,
  args: {
    uploadedRecordingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetUploadedRecordingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT UploadedRecordingTask.gcsFilename, UploadedRecordingTask.payload, UploadedRecordingTask.retryCount, UploadedRecordingTask.executionTimeMs, UploadedRecordingTask.createdTimeMs FROM UploadedRecordingTask WHERE (UploadedRecordingTask.gcsFilename = @uploadedRecordingTaskGcsFilenameEq)",
    params: {
      uploadedRecordingTaskGcsFilenameEq: args.uploadedRecordingTaskGcsFilenameEq,
    },
    types: {
      uploadedRecordingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetUploadedRecordingTaskRow>();
  for (let row of rows) {
    resRows.push({
      uploadedRecordingTaskGcsFilename: row.at(0).value == null ? undefined : row.at(0).value,
      uploadedRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, UPLOADED_RECORDING_TASK_PAYLOAD),
      uploadedRecordingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      uploadedRecordingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      uploadedRecordingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingUploadedRecordingTasksRow {
  uploadedRecordingTaskGcsFilename?: string,
  uploadedRecordingTaskPayload?: UploadedRecordingTaskPayload,
}

export let LIST_PENDING_UPLOADED_RECORDING_TASKS_ROW: MessageDescriptor<ListPendingUploadedRecordingTasksRow> = {
  name: 'ListPendingUploadedRecordingTasksRow',
  fields: [{
    name: 'uploadedRecordingTaskGcsFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'uploadedRecordingTaskPayload',
    index: 2,
    messageType: UPLOADED_RECORDING_TASK_PAYLOAD,
  }],
};

export async function listPendingUploadedRecordingTasks(
  runner: Database | Transaction,
  args: {
    uploadedRecordingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingUploadedRecordingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT UploadedRecordingTask.gcsFilename, UploadedRecordingTask.payload FROM UploadedRecordingTask WHERE UploadedRecordingTask.executionTimeMs <= @uploadedRecordingTaskExecutionTimeMsLe",
    params: {
      uploadedRecordingTaskExecutionTimeMsLe: args.uploadedRecordingTaskExecutionTimeMsLe == null ? null : new Date(args.uploadedRecordingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      uploadedRecordingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingUploadedRecordingTasksRow>();
  for (let row of rows) {
    resRows.push({
      uploadedRecordingTaskGcsFilename: row.at(0).value == null ? undefined : row.at(0).value,
      uploadedRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, UPLOADED_RECORDING_TASK_PAYLOAD),
    });
  }
  return resRows;
}

export interface GetUploadedRecordingTaskMetadataRow {
  uploadedRecordingTaskRetryCount?: number,
  uploadedRecordingTaskExecutionTimeMs?: number,
}

export let GET_UPLOADED_RECORDING_TASK_METADATA_ROW: MessageDescriptor<GetUploadedRecordingTaskMetadataRow> = {
  name: 'GetUploadedRecordingTaskMetadataRow',
  fields: [{
    name: 'uploadedRecordingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'uploadedRecordingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getUploadedRecordingTaskMetadata(
  runner: Database | Transaction,
  args: {
    uploadedRecordingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetUploadedRecordingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT UploadedRecordingTask.retryCount, UploadedRecordingTask.executionTimeMs FROM UploadedRecordingTask WHERE (UploadedRecordingTask.gcsFilename = @uploadedRecordingTaskGcsFilenameEq)",
    params: {
      uploadedRecordingTaskGcsFilenameEq: args.uploadedRecordingTaskGcsFilenameEq,
    },
    types: {
      uploadedRecordingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetUploadedRecordingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      uploadedRecordingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      uploadedRecordingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateUploadedRecordingTaskMetadataStatement(
  args: {
    uploadedRecordingTaskGcsFilenameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE UploadedRecordingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (UploadedRecordingTask.gcsFilename = @uploadedRecordingTaskGcsFilenameEq)",
    params: {
      uploadedRecordingTaskGcsFilenameEq: args.uploadedRecordingTaskGcsFilenameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      uploadedRecordingTaskGcsFilenameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertMediaFormattingTaskStatement(
  args: {
    containerId: string,
    gcsFilename: string,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT MediaFormattingTask (containerId, gcsFilename, retryCount, executionTimeMs, createdTimeMs) VALUES (@containerId, @gcsFilename, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: args.containerId,
      gcsFilename: args.gcsFilename,
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteMediaFormattingTaskStatement(
  args: {
    mediaFormattingTaskContainerIdEq: string,
    mediaFormattingTaskGcsFilenameEq: string,
  }
): Statement {
  return {
    sql: "DELETE MediaFormattingTask WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: args.mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: args.mediaFormattingTaskGcsFilenameEq,
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export interface GetMediaFormattingTaskRow {
  mediaFormattingTaskContainerId?: string,
  mediaFormattingTaskGcsFilename?: string,
  mediaFormattingTaskRetryCount?: number,
  mediaFormattingTaskExecutionTimeMs?: number,
  mediaFormattingTaskCreatedTimeMs?: number,
}

export let GET_MEDIA_FORMATTING_TASK_ROW: MessageDescriptor<GetMediaFormattingTaskRow> = {
  name: 'GetMediaFormattingTaskRow',
  fields: [{
    name: 'mediaFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'mediaFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'mediaFormattingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'mediaFormattingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'mediaFormattingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getMediaFormattingTask(
  runner: Database | Transaction,
  args: {
    mediaFormattingTaskContainerIdEq: string,
    mediaFormattingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetMediaFormattingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.containerId, MediaFormattingTask.gcsFilename, MediaFormattingTask.retryCount, MediaFormattingTask.executionTimeMs, MediaFormattingTask.createdTimeMs FROM MediaFormattingTask WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: args.mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: args.mediaFormattingTaskGcsFilenameEq,
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetMediaFormattingTaskRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      mediaFormattingTaskGcsFilename: row.at(1).value == null ? undefined : row.at(1).value,
      mediaFormattingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      mediaFormattingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      mediaFormattingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingMediaFormattingTasksRow {
  mediaFormattingTaskContainerId?: string,
  mediaFormattingTaskGcsFilename?: string,
}

export let LIST_PENDING_MEDIA_FORMATTING_TASKS_ROW: MessageDescriptor<ListPendingMediaFormattingTasksRow> = {
  name: 'ListPendingMediaFormattingTasksRow',
  fields: [{
    name: 'mediaFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'mediaFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function listPendingMediaFormattingTasks(
  runner: Database | Transaction,
  args: {
    mediaFormattingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingMediaFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.containerId, MediaFormattingTask.gcsFilename FROM MediaFormattingTask WHERE MediaFormattingTask.executionTimeMs <= @mediaFormattingTaskExecutionTimeMsLe",
    params: {
      mediaFormattingTaskExecutionTimeMsLe: args.mediaFormattingTaskExecutionTimeMsLe == null ? null : new Date(args.mediaFormattingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      mediaFormattingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingMediaFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      mediaFormattingTaskGcsFilename: row.at(1).value == null ? undefined : row.at(1).value,
    });
  }
  return resRows;
}

export interface GetMediaFormattingTaskMetadataRow {
  mediaFormattingTaskRetryCount?: number,
  mediaFormattingTaskExecutionTimeMs?: number,
}

export let GET_MEDIA_FORMATTING_TASK_METADATA_ROW: MessageDescriptor<GetMediaFormattingTaskMetadataRow> = {
  name: 'GetMediaFormattingTaskMetadataRow',
  fields: [{
    name: 'mediaFormattingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'mediaFormattingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getMediaFormattingTaskMetadata(
  runner: Database | Transaction,
  args: {
    mediaFormattingTaskContainerIdEq: string,
    mediaFormattingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetMediaFormattingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.retryCount, MediaFormattingTask.executionTimeMs FROM MediaFormattingTask WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: args.mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: args.mediaFormattingTaskGcsFilenameEq,
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetMediaFormattingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      mediaFormattingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateMediaFormattingTaskMetadataStatement(
  args: {
    mediaFormattingTaskContainerIdEq: string,
    mediaFormattingTaskGcsFilenameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE MediaFormattingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: args.mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: args.mediaFormattingTaskGcsFilenameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertSubtitleFormattingTaskStatement(
  args: {
    containerId: string,
    gcsFilename: string,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT SubtitleFormattingTask (containerId, gcsFilename, retryCount, executionTimeMs, createdTimeMs) VALUES (@containerId, @gcsFilename, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: args.containerId,
      gcsFilename: args.gcsFilename,
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteSubtitleFormattingTaskStatement(
  args: {
    subtitleFormattingTaskContainerIdEq: string,
    subtitleFormattingTaskGcsFilenameEq: string,
  }
): Statement {
  return {
    sql: "DELETE SubtitleFormattingTask WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: args.subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: args.subtitleFormattingTaskGcsFilenameEq,
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export interface GetSubtitleFormattingTaskRow {
  subtitleFormattingTaskContainerId?: string,
  subtitleFormattingTaskGcsFilename?: string,
  subtitleFormattingTaskRetryCount?: number,
  subtitleFormattingTaskExecutionTimeMs?: number,
  subtitleFormattingTaskCreatedTimeMs?: number,
}

export let GET_SUBTITLE_FORMATTING_TASK_ROW: MessageDescriptor<GetSubtitleFormattingTaskRow> = {
  name: 'GetSubtitleFormattingTaskRow',
  fields: [{
    name: 'subtitleFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'subtitleFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'subtitleFormattingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'subtitleFormattingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'subtitleFormattingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getSubtitleFormattingTask(
  runner: Database | Transaction,
  args: {
    subtitleFormattingTaskContainerIdEq: string,
    subtitleFormattingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetSubtitleFormattingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.gcsFilename, SubtitleFormattingTask.retryCount, SubtitleFormattingTask.executionTimeMs, SubtitleFormattingTask.createdTimeMs FROM SubtitleFormattingTask WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: args.subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: args.subtitleFormattingTaskGcsFilenameEq,
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetSubtitleFormattingTaskRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      subtitleFormattingTaskGcsFilename: row.at(1).value == null ? undefined : row.at(1).value,
      subtitleFormattingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      subtitleFormattingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      subtitleFormattingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingSubtitleFormattingTasksRow {
  subtitleFormattingTaskContainerId?: string,
  subtitleFormattingTaskGcsFilename?: string,
}

export let LIST_PENDING_SUBTITLE_FORMATTING_TASKS_ROW: MessageDescriptor<ListPendingSubtitleFormattingTasksRow> = {
  name: 'ListPendingSubtitleFormattingTasksRow',
  fields: [{
    name: 'subtitleFormattingTaskContainerId',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'subtitleFormattingTaskGcsFilename',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function listPendingSubtitleFormattingTasks(
  runner: Database | Transaction,
  args: {
    subtitleFormattingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingSubtitleFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.gcsFilename FROM SubtitleFormattingTask WHERE SubtitleFormattingTask.executionTimeMs <= @subtitleFormattingTaskExecutionTimeMsLe",
    params: {
      subtitleFormattingTaskExecutionTimeMsLe: args.subtitleFormattingTaskExecutionTimeMsLe == null ? null : new Date(args.subtitleFormattingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      subtitleFormattingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingSubtitleFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskContainerId: row.at(0).value == null ? undefined : row.at(0).value,
      subtitleFormattingTaskGcsFilename: row.at(1).value == null ? undefined : row.at(1).value,
    });
  }
  return resRows;
}

export interface GetSubtitleFormattingTaskMetadataRow {
  subtitleFormattingTaskRetryCount?: number,
  subtitleFormattingTaskExecutionTimeMs?: number,
}

export let GET_SUBTITLE_FORMATTING_TASK_METADATA_ROW: MessageDescriptor<GetSubtitleFormattingTaskMetadataRow> = {
  name: 'GetSubtitleFormattingTaskMetadataRow',
  fields: [{
    name: 'subtitleFormattingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'subtitleFormattingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getSubtitleFormattingTaskMetadata(
  runner: Database | Transaction,
  args: {
    subtitleFormattingTaskContainerIdEq: string,
    subtitleFormattingTaskGcsFilenameEq: string,
  }
): Promise<Array<GetSubtitleFormattingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.retryCount, SubtitleFormattingTask.executionTimeMs FROM SubtitleFormattingTask WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: args.subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: args.subtitleFormattingTaskGcsFilenameEq,
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetSubtitleFormattingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      subtitleFormattingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateSubtitleFormattingTaskMetadataStatement(
  args: {
    subtitleFormattingTaskContainerIdEq: string,
    subtitleFormattingTaskGcsFilenameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE SubtitleFormattingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: args.subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: args.subtitleFormattingTaskGcsFilenameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertStorageStartRecordingTaskStatement(
  args: {
    r2Dirname: string,
    payload: StorageStartRecordingTaskPayload,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT StorageStartRecordingTask (r2Dirname, payload, retryCount, executionTimeMs, createdTimeMs) VALUES (@r2Dirname, @payload, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      r2Dirname: args.r2Dirname,
      payload: Buffer.from(serializeMessage(args.payload, STORAGE_START_RECORDING_TASK_PAYLOAD).buffer),
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      r2Dirname: { type: "string" },
      payload: { type: "bytes" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteStorageStartRecordingTaskStatement(
  args: {
    storageStartRecordingTaskR2DirnameEq: string,
  }
): Statement {
  return {
    sql: "DELETE StorageStartRecordingTask WHERE (StorageStartRecordingTask.r2Dirname = @storageStartRecordingTaskR2DirnameEq)",
    params: {
      storageStartRecordingTaskR2DirnameEq: args.storageStartRecordingTaskR2DirnameEq,
    },
    types: {
      storageStartRecordingTaskR2DirnameEq: { type: "string" },
    }
  };
}

export interface GetStorageStartRecordingTaskRow {
  storageStartRecordingTaskR2Dirname?: string,
  storageStartRecordingTaskPayload?: StorageStartRecordingTaskPayload,
  storageStartRecordingTaskRetryCount?: number,
  storageStartRecordingTaskExecutionTimeMs?: number,
  storageStartRecordingTaskCreatedTimeMs?: number,
}

export let GET_STORAGE_START_RECORDING_TASK_ROW: MessageDescriptor<GetStorageStartRecordingTaskRow> = {
  name: 'GetStorageStartRecordingTaskRow',
  fields: [{
    name: 'storageStartRecordingTaskR2Dirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'storageStartRecordingTaskPayload',
    index: 2,
    messageType: STORAGE_START_RECORDING_TASK_PAYLOAD,
  }, {
    name: 'storageStartRecordingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageStartRecordingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageStartRecordingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getStorageStartRecordingTask(
  runner: Database | Transaction,
  args: {
    storageStartRecordingTaskR2DirnameEq: string,
  }
): Promise<Array<GetStorageStartRecordingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageStartRecordingTask.r2Dirname, StorageStartRecordingTask.payload, StorageStartRecordingTask.retryCount, StorageStartRecordingTask.executionTimeMs, StorageStartRecordingTask.createdTimeMs FROM StorageStartRecordingTask WHERE (StorageStartRecordingTask.r2Dirname = @storageStartRecordingTaskR2DirnameEq)",
    params: {
      storageStartRecordingTaskR2DirnameEq: args.storageStartRecordingTaskR2DirnameEq,
    },
    types: {
      storageStartRecordingTaskR2DirnameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetStorageStartRecordingTaskRow>();
  for (let row of rows) {
    resRows.push({
      storageStartRecordingTaskR2Dirname: row.at(0).value == null ? undefined : row.at(0).value,
      storageStartRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, STORAGE_START_RECORDING_TASK_PAYLOAD),
      storageStartRecordingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      storageStartRecordingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      storageStartRecordingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingStorageStartRecordingTasksRow {
  storageStartRecordingTaskR2Dirname?: string,
  storageStartRecordingTaskPayload?: StorageStartRecordingTaskPayload,
}

export let LIST_PENDING_STORAGE_START_RECORDING_TASKS_ROW: MessageDescriptor<ListPendingStorageStartRecordingTasksRow> = {
  name: 'ListPendingStorageStartRecordingTasksRow',
  fields: [{
    name: 'storageStartRecordingTaskR2Dirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'storageStartRecordingTaskPayload',
    index: 2,
    messageType: STORAGE_START_RECORDING_TASK_PAYLOAD,
  }],
};

export async function listPendingStorageStartRecordingTasks(
  runner: Database | Transaction,
  args: {
    storageStartRecordingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingStorageStartRecordingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageStartRecordingTask.r2Dirname, StorageStartRecordingTask.payload FROM StorageStartRecordingTask WHERE StorageStartRecordingTask.executionTimeMs <= @storageStartRecordingTaskExecutionTimeMsLe",
    params: {
      storageStartRecordingTaskExecutionTimeMsLe: args.storageStartRecordingTaskExecutionTimeMsLe == null ? null : new Date(args.storageStartRecordingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      storageStartRecordingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingStorageStartRecordingTasksRow>();
  for (let row of rows) {
    resRows.push({
      storageStartRecordingTaskR2Dirname: row.at(0).value == null ? undefined : row.at(0).value,
      storageStartRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, STORAGE_START_RECORDING_TASK_PAYLOAD),
    });
  }
  return resRows;
}

export interface GetStorageStartRecordingTaskMetadataRow {
  storageStartRecordingTaskRetryCount?: number,
  storageStartRecordingTaskExecutionTimeMs?: number,
}

export let GET_STORAGE_START_RECORDING_TASK_METADATA_ROW: MessageDescriptor<GetStorageStartRecordingTaskMetadataRow> = {
  name: 'GetStorageStartRecordingTaskMetadataRow',
  fields: [{
    name: 'storageStartRecordingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageStartRecordingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getStorageStartRecordingTaskMetadata(
  runner: Database | Transaction,
  args: {
    storageStartRecordingTaskR2DirnameEq: string,
  }
): Promise<Array<GetStorageStartRecordingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageStartRecordingTask.retryCount, StorageStartRecordingTask.executionTimeMs FROM StorageStartRecordingTask WHERE (StorageStartRecordingTask.r2Dirname = @storageStartRecordingTaskR2DirnameEq)",
    params: {
      storageStartRecordingTaskR2DirnameEq: args.storageStartRecordingTaskR2DirnameEq,
    },
    types: {
      storageStartRecordingTaskR2DirnameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetStorageStartRecordingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      storageStartRecordingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      storageStartRecordingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateStorageStartRecordingTaskMetadataStatement(
  args: {
    storageStartRecordingTaskR2DirnameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE StorageStartRecordingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (StorageStartRecordingTask.r2Dirname = @storageStartRecordingTaskR2DirnameEq)",
    params: {
      storageStartRecordingTaskR2DirnameEq: args.storageStartRecordingTaskR2DirnameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      storageStartRecordingTaskR2DirnameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertStorageEndRecordingTaskStatement(
  args: {
    r2Dirname: string,
    payload: StorageEndRecordingTaskPayload,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT StorageEndRecordingTask (r2Dirname, payload, retryCount, executionTimeMs, createdTimeMs) VALUES (@r2Dirname, @payload, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      r2Dirname: args.r2Dirname,
      payload: Buffer.from(serializeMessage(args.payload, STORAGE_END_RECORDING_TASK_PAYLOAD).buffer),
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      r2Dirname: { type: "string" },
      payload: { type: "bytes" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteStorageEndRecordingTaskStatement(
  args: {
    storageEndRecordingTaskR2DirnameEq: string,
  }
): Statement {
  return {
    sql: "DELETE StorageEndRecordingTask WHERE (StorageEndRecordingTask.r2Dirname = @storageEndRecordingTaskR2DirnameEq)",
    params: {
      storageEndRecordingTaskR2DirnameEq: args.storageEndRecordingTaskR2DirnameEq,
    },
    types: {
      storageEndRecordingTaskR2DirnameEq: { type: "string" },
    }
  };
}

export interface GetStorageEndRecordingTaskRow {
  storageEndRecordingTaskR2Dirname?: string,
  storageEndRecordingTaskPayload?: StorageEndRecordingTaskPayload,
  storageEndRecordingTaskRetryCount?: number,
  storageEndRecordingTaskExecutionTimeMs?: number,
  storageEndRecordingTaskCreatedTimeMs?: number,
}

export let GET_STORAGE_END_RECORDING_TASK_ROW: MessageDescriptor<GetStorageEndRecordingTaskRow> = {
  name: 'GetStorageEndRecordingTaskRow',
  fields: [{
    name: 'storageEndRecordingTaskR2Dirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'storageEndRecordingTaskPayload',
    index: 2,
    messageType: STORAGE_END_RECORDING_TASK_PAYLOAD,
  }, {
    name: 'storageEndRecordingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageEndRecordingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageEndRecordingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getStorageEndRecordingTask(
  runner: Database | Transaction,
  args: {
    storageEndRecordingTaskR2DirnameEq: string,
  }
): Promise<Array<GetStorageEndRecordingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageEndRecordingTask.r2Dirname, StorageEndRecordingTask.payload, StorageEndRecordingTask.retryCount, StorageEndRecordingTask.executionTimeMs, StorageEndRecordingTask.createdTimeMs FROM StorageEndRecordingTask WHERE (StorageEndRecordingTask.r2Dirname = @storageEndRecordingTaskR2DirnameEq)",
    params: {
      storageEndRecordingTaskR2DirnameEq: args.storageEndRecordingTaskR2DirnameEq,
    },
    types: {
      storageEndRecordingTaskR2DirnameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetStorageEndRecordingTaskRow>();
  for (let row of rows) {
    resRows.push({
      storageEndRecordingTaskR2Dirname: row.at(0).value == null ? undefined : row.at(0).value,
      storageEndRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, STORAGE_END_RECORDING_TASK_PAYLOAD),
      storageEndRecordingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      storageEndRecordingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      storageEndRecordingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingStorageEndRecordingTasksRow {
  storageEndRecordingTaskR2Dirname?: string,
  storageEndRecordingTaskPayload?: StorageEndRecordingTaskPayload,
}

export let LIST_PENDING_STORAGE_END_RECORDING_TASKS_ROW: MessageDescriptor<ListPendingStorageEndRecordingTasksRow> = {
  name: 'ListPendingStorageEndRecordingTasksRow',
  fields: [{
    name: 'storageEndRecordingTaskR2Dirname',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'storageEndRecordingTaskPayload',
    index: 2,
    messageType: STORAGE_END_RECORDING_TASK_PAYLOAD,
  }],
};

export async function listPendingStorageEndRecordingTasks(
  runner: Database | Transaction,
  args: {
    storageEndRecordingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingStorageEndRecordingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageEndRecordingTask.r2Dirname, StorageEndRecordingTask.payload FROM StorageEndRecordingTask WHERE StorageEndRecordingTask.executionTimeMs <= @storageEndRecordingTaskExecutionTimeMsLe",
    params: {
      storageEndRecordingTaskExecutionTimeMsLe: args.storageEndRecordingTaskExecutionTimeMsLe == null ? null : new Date(args.storageEndRecordingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      storageEndRecordingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingStorageEndRecordingTasksRow>();
  for (let row of rows) {
    resRows.push({
      storageEndRecordingTaskR2Dirname: row.at(0).value == null ? undefined : row.at(0).value,
      storageEndRecordingTaskPayload: row.at(1).value == null ? undefined : deserializeMessage(row.at(1).value, STORAGE_END_RECORDING_TASK_PAYLOAD),
    });
  }
  return resRows;
}

export interface GetStorageEndRecordingTaskMetadataRow {
  storageEndRecordingTaskRetryCount?: number,
  storageEndRecordingTaskExecutionTimeMs?: number,
}

export let GET_STORAGE_END_RECORDING_TASK_METADATA_ROW: MessageDescriptor<GetStorageEndRecordingTaskMetadataRow> = {
  name: 'GetStorageEndRecordingTaskMetadataRow',
  fields: [{
    name: 'storageEndRecordingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'storageEndRecordingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getStorageEndRecordingTaskMetadata(
  runner: Database | Transaction,
  args: {
    storageEndRecordingTaskR2DirnameEq: string,
  }
): Promise<Array<GetStorageEndRecordingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT StorageEndRecordingTask.retryCount, StorageEndRecordingTask.executionTimeMs FROM StorageEndRecordingTask WHERE (StorageEndRecordingTask.r2Dirname = @storageEndRecordingTaskR2DirnameEq)",
    params: {
      storageEndRecordingTaskR2DirnameEq: args.storageEndRecordingTaskR2DirnameEq,
    },
    types: {
      storageEndRecordingTaskR2DirnameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetStorageEndRecordingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      storageEndRecordingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      storageEndRecordingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateStorageEndRecordingTaskMetadataStatement(
  args: {
    storageEndRecordingTaskR2DirnameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE StorageEndRecordingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (StorageEndRecordingTask.r2Dirname = @storageEndRecordingTaskR2DirnameEq)",
    params: {
      storageEndRecordingTaskR2DirnameEq: args.storageEndRecordingTaskR2DirnameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      storageEndRecordingTaskR2DirnameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertGcsFileDeletingTaskStatement(
  args: {
    filename: string,
    uploadSessionUrl: string,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT GcsFileDeletingTask (filename, uploadSessionUrl, retryCount, executionTimeMs, createdTimeMs) VALUES (@filename, @uploadSessionUrl, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      filename: args.filename,
      uploadSessionUrl: args.uploadSessionUrl,
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      filename: { type: "string" },
      uploadSessionUrl: { type: "string" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteGcsFileDeletingTaskStatement(
  args: {
    gcsFileDeletingTaskFilenameEq: string,
  }
): Statement {
  return {
    sql: "DELETE GcsFileDeletingTask WHERE (GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq)",
    params: {
      gcsFileDeletingTaskFilenameEq: args.gcsFileDeletingTaskFilenameEq,
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
    }
  };
}

export interface GetGcsFileDeletingTaskRow {
  gcsFileDeletingTaskFilename?: string,
  gcsFileDeletingTaskUploadSessionUrl?: string,
  gcsFileDeletingTaskRetryCount?: number,
  gcsFileDeletingTaskExecutionTimeMs?: number,
  gcsFileDeletingTaskCreatedTimeMs?: number,
}

export let GET_GCS_FILE_DELETING_TASK_ROW: MessageDescriptor<GetGcsFileDeletingTaskRow> = {
  name: 'GetGcsFileDeletingTaskRow',
  fields: [{
    name: 'gcsFileDeletingTaskFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskUploadSessionUrl',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskRetryCount',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'gcsFileDeletingTaskExecutionTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'gcsFileDeletingTaskCreatedTimeMs',
    index: 5,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getGcsFileDeletingTask(
  runner: Database | Transaction,
  args: {
    gcsFileDeletingTaskFilenameEq: string,
  }
): Promise<Array<GetGcsFileDeletingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeletingTask.filename, GcsFileDeletingTask.uploadSessionUrl, GcsFileDeletingTask.retryCount, GcsFileDeletingTask.executionTimeMs, GcsFileDeletingTask.createdTimeMs FROM GcsFileDeletingTask WHERE (GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq)",
    params: {
      gcsFileDeletingTaskFilenameEq: args.gcsFileDeletingTaskFilenameEq,
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetGcsFileDeletingTaskRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeletingTaskFilename: row.at(0).value == null ? undefined : row.at(0).value,
      gcsFileDeletingTaskUploadSessionUrl: row.at(1).value == null ? undefined : row.at(1).value,
      gcsFileDeletingTaskRetryCount: row.at(2).value == null ? undefined : row.at(2).value.value,
      gcsFileDeletingTaskExecutionTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
      gcsFileDeletingTaskCreatedTimeMs: row.at(4).value == null ? undefined : row.at(4).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingGcsFileDeletingTasksRow {
  gcsFileDeletingTaskFilename?: string,
  gcsFileDeletingTaskUploadSessionUrl?: string,
}

export let LIST_PENDING_GCS_FILE_DELETING_TASKS_ROW: MessageDescriptor<ListPendingGcsFileDeletingTasksRow> = {
  name: 'ListPendingGcsFileDeletingTasksRow',
  fields: [{
    name: 'gcsFileDeletingTaskFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskUploadSessionUrl',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function listPendingGcsFileDeletingTasks(
  runner: Database | Transaction,
  args: {
    gcsFileDeletingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingGcsFileDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeletingTask.filename, GcsFileDeletingTask.uploadSessionUrl FROM GcsFileDeletingTask WHERE GcsFileDeletingTask.executionTimeMs <= @gcsFileDeletingTaskExecutionTimeMsLe",
    params: {
      gcsFileDeletingTaskExecutionTimeMsLe: args.gcsFileDeletingTaskExecutionTimeMsLe == null ? null : new Date(args.gcsFileDeletingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      gcsFileDeletingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingGcsFileDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeletingTaskFilename: row.at(0).value == null ? undefined : row.at(0).value,
      gcsFileDeletingTaskUploadSessionUrl: row.at(1).value == null ? undefined : row.at(1).value,
    });
  }
  return resRows;
}

export interface GetGcsFileDeletingTaskMetadataRow {
  gcsFileDeletingTaskRetryCount?: number,
  gcsFileDeletingTaskExecutionTimeMs?: number,
}

export let GET_GCS_FILE_DELETING_TASK_METADATA_ROW: MessageDescriptor<GetGcsFileDeletingTaskMetadataRow> = {
  name: 'GetGcsFileDeletingTaskMetadataRow',
  fields: [{
    name: 'gcsFileDeletingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'gcsFileDeletingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getGcsFileDeletingTaskMetadata(
  runner: Database | Transaction,
  args: {
    gcsFileDeletingTaskFilenameEq: string,
  }
): Promise<Array<GetGcsFileDeletingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeletingTask.retryCount, GcsFileDeletingTask.executionTimeMs FROM GcsFileDeletingTask WHERE (GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq)",
    params: {
      gcsFileDeletingTaskFilenameEq: args.gcsFileDeletingTaskFilenameEq,
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
    }
  });
  let resRows = new Array<GetGcsFileDeletingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeletingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      gcsFileDeletingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateGcsFileDeletingTaskMetadataStatement(
  args: {
    gcsFileDeletingTaskFilenameEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE GcsFileDeletingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq)",
    params: {
      gcsFileDeletingTaskFilenameEq: args.gcsFileDeletingTaskFilenameEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function insertR2KeyDeletingTaskStatement(
  args: {
    key: string,
    retryCount?: number,
    executionTimeMs?: number,
    createdTimeMs?: number,
  }
): Statement {
  return {
    sql: "INSERT R2KeyDeletingTask (key, retryCount, executionTimeMs, createdTimeMs) VALUES (@key, @retryCount, @executionTimeMs, @createdTimeMs)",
    params: {
      key: args.key,
      retryCount: args.retryCount == null ? null : Spanner.float(args.retryCount),
      executionTimeMs: args.executionTimeMs == null ? null : new Date(args.executionTimeMs).toISOString(),
      createdTimeMs: args.createdTimeMs == null ? null : new Date(args.createdTimeMs).toISOString(),
    },
    types: {
      key: { type: "string" },
      retryCount: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function deleteR2KeyDeletingTaskStatement(
  args: {
    r2KeyDeletingTaskKeyEq: string,
  }
): Statement {
  return {
    sql: "DELETE R2KeyDeletingTask WHERE (R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq)",
    params: {
      r2KeyDeletingTaskKeyEq: args.r2KeyDeletingTaskKeyEq,
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
    }
  };
}

export interface GetR2KeyDeletingTaskRow {
  r2KeyDeletingTaskKey?: string,
  r2KeyDeletingTaskRetryCount?: number,
  r2KeyDeletingTaskExecutionTimeMs?: number,
  r2KeyDeletingTaskCreatedTimeMs?: number,
}

export let GET_R2_KEY_DELETING_TASK_ROW: MessageDescriptor<GetR2KeyDeletingTaskRow> = {
  name: 'GetR2KeyDeletingTaskRow',
  fields: [{
    name: 'r2KeyDeletingTaskKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2KeyDeletingTaskRetryCount',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2KeyDeletingTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2KeyDeletingTaskCreatedTimeMs',
    index: 4,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getR2KeyDeletingTask(
  runner: Database | Transaction,
  args: {
    r2KeyDeletingTaskKeyEq: string,
  }
): Promise<Array<GetR2KeyDeletingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeletingTask.key, R2KeyDeletingTask.retryCount, R2KeyDeletingTask.executionTimeMs, R2KeyDeletingTask.createdTimeMs FROM R2KeyDeletingTask WHERE (R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq)",
    params: {
      r2KeyDeletingTaskKeyEq: args.r2KeyDeletingTaskKeyEq,
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
    }
  });
  let resRows = new Array<GetR2KeyDeletingTaskRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeletingTaskKey: row.at(0).value == null ? undefined : row.at(0).value,
      r2KeyDeletingTaskRetryCount: row.at(1).value == null ? undefined : row.at(1).value.value,
      r2KeyDeletingTaskExecutionTimeMs: row.at(2).value == null ? undefined : row.at(2).value.valueOf(),
      r2KeyDeletingTaskCreatedTimeMs: row.at(3).value == null ? undefined : row.at(3).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListPendingR2KeyDeletingTasksRow {
  r2KeyDeletingTaskKey?: string,
}

export let LIST_PENDING_R2_KEY_DELETING_TASKS_ROW: MessageDescriptor<ListPendingR2KeyDeletingTasksRow> = {
  name: 'ListPendingR2KeyDeletingTasksRow',
  fields: [{
    name: 'r2KeyDeletingTaskKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }],
};

export async function listPendingR2KeyDeletingTasks(
  runner: Database | Transaction,
  args: {
    r2KeyDeletingTaskExecutionTimeMsLe?: number,
  }
): Promise<Array<ListPendingR2KeyDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeletingTask.key FROM R2KeyDeletingTask WHERE R2KeyDeletingTask.executionTimeMs <= @r2KeyDeletingTaskExecutionTimeMsLe",
    params: {
      r2KeyDeletingTaskExecutionTimeMsLe: args.r2KeyDeletingTaskExecutionTimeMsLe == null ? null : new Date(args.r2KeyDeletingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      r2KeyDeletingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListPendingR2KeyDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeletingTaskKey: row.at(0).value == null ? undefined : row.at(0).value,
    });
  }
  return resRows;
}

export interface GetR2KeyDeletingTaskMetadataRow {
  r2KeyDeletingTaskRetryCount?: number,
  r2KeyDeletingTaskExecutionTimeMs?: number,
}

export let GET_R2_KEY_DELETING_TASK_METADATA_ROW: MessageDescriptor<GetR2KeyDeletingTaskMetadataRow> = {
  name: 'GetR2KeyDeletingTaskMetadataRow',
  fields: [{
    name: 'r2KeyDeletingTaskRetryCount',
    index: 1,
    primitiveType: PrimitiveType.NUMBER,
  }, {
    name: 'r2KeyDeletingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function getR2KeyDeletingTaskMetadata(
  runner: Database | Transaction,
  args: {
    r2KeyDeletingTaskKeyEq: string,
  }
): Promise<Array<GetR2KeyDeletingTaskMetadataRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeletingTask.retryCount, R2KeyDeletingTask.executionTimeMs FROM R2KeyDeletingTask WHERE (R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq)",
    params: {
      r2KeyDeletingTaskKeyEq: args.r2KeyDeletingTaskKeyEq,
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
    }
  });
  let resRows = new Array<GetR2KeyDeletingTaskMetadataRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeletingTaskRetryCount: row.at(0).value == null ? undefined : row.at(0).value.value,
      r2KeyDeletingTaskExecutionTimeMs: row.at(1).value == null ? undefined : row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export function updateR2KeyDeletingTaskMetadataStatement(
  args: {
    r2KeyDeletingTaskKeyEq: string,
    setRetryCount?: number,
    setExecutionTimeMs?: number,
  }
): Statement {
  return {
    sql: "UPDATE R2KeyDeletingTask SET retryCount = @setRetryCount, executionTimeMs = @setExecutionTimeMs WHERE (R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq)",
    params: {
      r2KeyDeletingTaskKeyEq: args.r2KeyDeletingTaskKeyEq,
      setRetryCount: args.setRetryCount == null ? null : Spanner.float(args.setRetryCount),
      setExecutionTimeMs: args.setExecutionTimeMs == null ? null : new Date(args.setExecutionTimeMs).toISOString(),
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
      setRetryCount: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateVideoContainerStatement(
  args: {
    videoContainerContainerIdEq: string,
    setData?: VideoContainer,
  }
): Statement {
  return {
    sql: "UPDATE VideoContainer SET data = @setData WHERE VideoContainer.containerId = @videoContainerContainerIdEq",
    params: {
      videoContainerContainerIdEq: args.videoContainerContainerIdEq,
      setData: args.setData == null ? null : Buffer.from(serializeMessage(args.setData, VIDEO_CONTAINER).buffer),
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
      setData: { type: "bytes" },
    }
  };
}
