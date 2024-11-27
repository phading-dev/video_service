import { VideoContainerData, VIDEO_CONTAINER_DATA } from './schema';
import { deserializeMessage, serializeMessage } from '@selfage/message/serializer';
import { Database, Transaction, Spanner } from '@google-cloud/spanner';
import { Statement } from '@google-cloud/spanner/build/src/transaction';

export interface GetVideoContainerRow {
  videoContainerData: VideoContainerData,
}

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

export interface GetVideoContainerSyncingTasksRow {
  videoContainerSyncingTaskContainerId: string,
  videoContainerSyncingTaskVersion: number,
  videoContainerSyncingTaskExecutionTimestamp: number,
}

export async function getVideoContainerSyncingTasks(
  runner: Database | Transaction,
  videoContainerSyncingTaskExecutionTimestampLt: number,
): Promise<Array<GetVideoContainerSyncingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.containerId, VideoContainerSyncingTask.version, VideoContainerSyncingTask.executionTimestamp FROM VideoContainerSyncingTask WHERE VideoContainerSyncingTask.executionTimestamp < @videoContainerSyncingTaskExecutionTimestampLt ORDER BY VideoContainerSyncingTask.executionTimestamp DESC",
    params: {
      videoContainerSyncingTaskExecutionTimestampLt: new Date(videoContainerSyncingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      videoContainerSyncingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetVideoContainerSyncingTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskContainerId: row.at(0).value,
      videoContainerSyncingTaskVersion: row.at(1).value.value,
      videoContainerSyncingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetMediaFormattingTasksRow {
  mediaFormattingTaskContainerId: string,
  mediaFormattingTaskGcsFilename: string,
  mediaFormattingTaskExecutionTimestamp: number,
}

export async function getMediaFormattingTasks(
  runner: Database | Transaction,
  mediaFormattingTaskExecutionTimestampLt: number,
): Promise<Array<GetMediaFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.containerId, MediaFormattingTask.gcsFilename, MediaFormattingTask.executionTimestamp FROM MediaFormattingTask WHERE MediaFormattingTask.executionTimestamp < @mediaFormattingTaskExecutionTimestampLt ORDER BY MediaFormattingTask.executionTimestamp DESC",
    params: {
      mediaFormattingTaskExecutionTimestampLt: new Date(mediaFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      mediaFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetMediaFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskContainerId: row.at(0).value,
      mediaFormattingTaskGcsFilename: row.at(1).value,
      mediaFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetSubtitleFormattingTasksRow {
  subtitleFormattingTaskContainerId: string,
  subtitleFormattingTaskGcsFilename: string,
  subtitleFormattingTaskExecutionTimestamp: number,
}

export async function getSubtitleFormattingTasks(
  runner: Database | Transaction,
  subtitleFormattingTaskExecutionTimestampLt: number,
): Promise<Array<GetSubtitleFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.gcsFilename, SubtitleFormattingTask.executionTimestamp FROM SubtitleFormattingTask WHERE SubtitleFormattingTask.executionTimestamp < @subtitleFormattingTaskExecutionTimestampLt ORDER BY SubtitleFormattingTask.executionTimestamp DESC",
    params: {
      subtitleFormattingTaskExecutionTimestampLt: new Date(subtitleFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      subtitleFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetSubtitleFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskContainerId: row.at(0).value,
      subtitleFormattingTaskGcsFilename: row.at(1).value,
      subtitleFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetGcsFileCleanupTasksRow {
  gcsFileCleanupTaskFilename: string,
  gcsFileCleanupTaskExecutionTimestamp: number,
}

export async function getGcsFileCleanupTasks(
  runner: Database | Transaction,
  gcsFileCleanupTaskExecutionTimestampLt: number,
): Promise<Array<GetGcsFileCleanupTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileCleanupTask.filename, GcsFileCleanupTask.executionTimestamp FROM GcsFileCleanupTask WHERE GcsFileCleanupTask.executionTimestamp < @gcsFileCleanupTaskExecutionTimestampLt ORDER BY GcsFileCleanupTask.executionTimestamp DESC",
    params: {
      gcsFileCleanupTaskExecutionTimestampLt: new Date(gcsFileCleanupTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      gcsFileCleanupTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetGcsFileCleanupTasksRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileCleanupTaskFilename: row.at(0).value,
      gcsFileCleanupTaskExecutionTimestamp: row.at(1).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetR2KeyCleanupTasksRow {
  r2KeyCleanupTaskKey: string,
  r2KeyCleanupTaskExecutionTimestamp: number,
}

export async function getR2KeyCleanupTasks(
  runner: Database | Transaction,
  r2KeyCleanupTaskExecutionTimestampLt: number,
): Promise<Array<GetR2KeyCleanupTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyCleanupTask.key, R2KeyCleanupTask.executionTimestamp FROM R2KeyCleanupTask WHERE R2KeyCleanupTask.executionTimestamp < @r2KeyCleanupTaskExecutionTimestampLt ORDER BY R2KeyCleanupTask.executionTimestamp DESC",
    params: {
      r2KeyCleanupTaskExecutionTimestampLt: new Date(r2KeyCleanupTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      r2KeyCleanupTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetR2KeyCleanupTasksRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyCleanupTaskKey: row.at(0).value,
      r2KeyCleanupTaskExecutionTimestamp: row.at(1).value.valueOf(),
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

export function insertGcsFileCleanupTaskStatement(
  filename: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT GcsFileCleanupTask (filename, executionTimestamp, createdTimestamp) VALUES (@filename, @executionTimestamp, @createdTimestamp)",
    params: {
      filename: filename,
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      filename: { type: "string" },
      executionTimestamp: { type: "timestamp" },
      createdTimestamp: { type: "timestamp" },
    }
  };
}

export function insertR2KeyCleanupTaskStatement(
  key: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT R2KeyCleanupTask (key, executionTimestamp, createdTimestamp) VALUES (@key, @executionTimestamp, @createdTimestamp)",
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
  setData: VideoContainerData,
  videoContainerContainerIdEq: string,
): Statement {
  return {
    sql: "UPDATE VideoContainer SET data = @setData WHERE VideoContainer.containerId = @videoContainerContainerIdEq",
    params: {
      setData: Buffer.from(serializeMessage(setData, VIDEO_CONTAINER_DATA).buffer),
      videoContainerContainerIdEq: videoContainerContainerIdEq,
    },
    types: {
      setData: { type: "bytes" },
      videoContainerContainerIdEq: { type: "string" },
    }
  };
}

export function delayVideoContainerSyncingTaskStatement(
  setExecutionTimestamp: number,
  videoContainerSyncingTaskContainerIdEq: string,
  videoContainerSyncingTaskVersionEq: number,
): Statement {
  return {
    sql: "UPDATE VideoContainerSyncingTask SET executionTimestamp = @setExecutionTimestamp WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      videoContainerSyncingTaskContainerIdEq: videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(videoContainerSyncingTaskVersionEq),
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
    }
  };
}

export function delayMediaFormattingTaskStatement(
  setExecutionTimestamp: number,
  mediaFormattingTaskContainerIdEq: string,
  mediaFormattingTaskGcsFilenameEq: string,
): Statement {
  return {
    sql: "UPDATE MediaFormattingTask SET executionTimestamp = @setExecutionTimestamp WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      mediaFormattingTaskContainerIdEq: mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: mediaFormattingTaskGcsFilenameEq,
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export function delaySubtitleFormattingTaskStatement(
  setExecutionTimestamp: number,
  subtitleFormattingTaskContainerIdEq: string,
  subtitleFormattingTaskGcsFilenameEq: string,
): Statement {
  return {
    sql: "UPDATE SubtitleFormattingTask SET executionTimestamp = @setExecutionTimestamp WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      subtitleFormattingTaskContainerIdEq: subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: subtitleFormattingTaskGcsFilenameEq,
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
    }
  };
}

export function delayGcsFileCleanupTaskStatement(
  setExecutionTimestamp: number,
  gcsFileCleanupTaskFilenameEq: string,
): Statement {
  return {
    sql: "UPDATE GcsFileCleanupTask SET executionTimestamp = @setExecutionTimestamp WHERE GcsFileCleanupTask.filename = @gcsFileCleanupTaskFilenameEq",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      gcsFileCleanupTaskFilenameEq: gcsFileCleanupTaskFilenameEq,
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      gcsFileCleanupTaskFilenameEq: { type: "string" },
    }
  };
}

export function delayR2KeyCleanupTaskStatement(
  setExecutionTimestamp: number,
  r2KeyCleanupTaskKeyEq: string,
): Statement {
  return {
    sql: "UPDATE R2KeyCleanupTask SET executionTimestamp = @setExecutionTimestamp WHERE R2KeyCleanupTask.key = @r2KeyCleanupTaskKeyEq",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      r2KeyCleanupTaskKeyEq: r2KeyCleanupTaskKeyEq,
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      r2KeyCleanupTaskKeyEq: { type: "string" },
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

export function deleteGcsFileCleanupTaskStatement(
  gcsFileCleanupTaskFilenameEq: string,
): Statement {
  return {
    sql: "DELETE GcsFileCleanupTask WHERE GcsFileCleanupTask.filename = @gcsFileCleanupTaskFilenameEq",
    params: {
      gcsFileCleanupTaskFilenameEq: gcsFileCleanupTaskFilenameEq,
    },
    types: {
      gcsFileCleanupTaskFilenameEq: { type: "string" },
    }
  };
}

export function deleteR2KeyCleanupTaskStatement(
  r2KeyCleanupTaskKeyEq: string,
): Statement {
  return {
    sql: "DELETE R2KeyCleanupTask WHERE R2KeyCleanupTask.key = @r2KeyCleanupTaskKeyEq",
    params: {
      r2KeyCleanupTaskKeyEq: r2KeyCleanupTaskKeyEq,
    },
    types: {
      r2KeyCleanupTaskKeyEq: { type: "string" },
    }
  };
}
