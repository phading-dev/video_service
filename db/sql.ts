import { Statement } from '@google-cloud/spanner/build/src/transaction';
import { VideoContainer, VIDEO_CONTAINER } from './schema';
import { serializeMessage, deserializeMessage } from '@selfage/message/serializer';
import { Database, Transaction, Spanner } from '@google-cloud/spanner';
import { MessageDescriptor, PrimitiveType } from '@selfage/message/descriptor';

export function insertVideoContainerStatement(
  data: VideoContainer,
): Statement {
  return insertVideoContainerInternalStatement(
    data.containerId,
    data
  );
}

export function insertVideoContainerInternalStatement(
  containerId: string,
  data: VideoContainer,
): Statement {
  return {
    sql: "INSERT VideoContainer (containerId, data) VALUES (@containerId, @data)",
    params: {
      containerId: containerId,
      data: Buffer.from(serializeMessage(data, VIDEO_CONTAINER).buffer),
    },
    types: {
      containerId: { type: "string" },
      data: { type: "bytes" },
    }
  };
}

export function deleteVideoContainerStatement(
  videoContainerContainerIdEq: string,
): Statement {
  return {
    sql: "DELETE VideoContainer WHERE (VideoContainer.containerId = @videoContainerContainerIdEq)",
    params: {
      videoContainerContainerIdEq: videoContainerContainerIdEq,
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
    }
  };
}

export interface GetVideoContainerRow {
  videoContainerData: VideoContainer,
}

export let GET_VIDEO_CONTAINER_ROW: MessageDescriptor<GetVideoContainerRow> = {
  name: 'GetVideoContainerRow',
  fields: [{
    name: 'videoContainerData',
    index: 1,
    messageType: VIDEO_CONTAINER,
  }],
};

export async function getVideoContainer(
  runner: Database | Transaction,
  videoContainerContainerIdEq: string,
): Promise<Array<GetVideoContainerRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainer.data FROM VideoContainer WHERE (VideoContainer.containerId = @videoContainerContainerIdEq)",
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
      videoContainerData: deserializeMessage(row.at(0).value, VIDEO_CONTAINER),
    });
  }
  return resRows;
}

export function updateVideoContainerStatement(
  data: VideoContainer,
): Statement {
  return updateVideoContainerInternalStatement(
    data.containerId,
    data
  );
}

export function updateVideoContainerInternalStatement(
  videoContainerContainerIdEq: string,
  setData: VideoContainer,
): Statement {
  return {
    sql: "UPDATE VideoContainer SET data = @setData WHERE (VideoContainer.containerId = @videoContainerContainerIdEq)",
    params: {
      videoContainerContainerIdEq: videoContainerContainerIdEq,
      setData: Buffer.from(serializeMessage(setData, VIDEO_CONTAINER).buffer),
    },
    types: {
      videoContainerContainerIdEq: { type: "string" },
      setData: { type: "bytes" },
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
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT VideoContainerWritingToFileTask (containerId, version, executionTimeMs, createdTimeMs) VALUES (@containerId, @version, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: containerId,
      version: Spanner.float(version),
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function insertVideoContainerSyncingTaskStatement(
  containerId: string,
  version: number,
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT VideoContainerSyncingTask (containerId, version, executionTimeMs, createdTimeMs) VALUES (@containerId, @version, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: containerId,
      version: Spanner.float(version),
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      version: { type: "float64" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function insertMediaFormattingTaskStatement(
  containerId: string,
  gcsFilename: string,
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT MediaFormattingTask (containerId, gcsFilename, executionTimeMs, createdTimeMs) VALUES (@containerId, @gcsFilename, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: containerId,
      gcsFilename: gcsFilename,
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function insertSubtitleFormattingTaskStatement(
  containerId: string,
  gcsFilename: string,
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT SubtitleFormattingTask (containerId, gcsFilename, executionTimeMs, createdTimeMs) VALUES (@containerId, @gcsFilename, @executionTimeMs, @createdTimeMs)",
    params: {
      containerId: containerId,
      gcsFilename: gcsFilename,
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      gcsFilename: { type: "string" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function insertGcsFileDeletingTaskStatement(
  filename: string,
  uploadSessionUrl: string,
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT GcsFileDeletingTask (filename, uploadSessionUrl, executionTimeMs, createdTimeMs) VALUES (@filename, @uploadSessionUrl, @executionTimeMs, @createdTimeMs)",
    params: {
      filename: filename,
      uploadSessionUrl: uploadSessionUrl,
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      filename: { type: "string" },
      uploadSessionUrl: { type: "string" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function insertR2KeyDeletingTaskStatement(
  key: string,
  executionTimeMs: number,
  createdTimeMs: number,
): Statement {
  return {
    sql: "INSERT R2KeyDeletingTask (key, executionTimeMs, createdTimeMs) VALUES (@key, @executionTimeMs, @createdTimeMs)",
    params: {
      key: key,
      executionTimeMs: new Date(executionTimeMs).toISOString(),
      createdTimeMs: new Date(createdTimeMs).toISOString(),
    },
    types: {
      key: { type: "string" },
      executionTimeMs: { type: "timestamp" },
      createdTimeMs: { type: "timestamp" },
    }
  };
}

export function updateVideoContainerWritingToFileTaskStatement(
  videoContainerWritingToFileTaskContainerIdEq: string,
  videoContainerWritingToFileTaskVersionEq: number,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE VideoContainerWritingToFileTask SET executionTimeMs = @setExecutionTimeMs WHERE (VideoContainerWritingToFileTask.containerId = @videoContainerWritingToFileTaskContainerIdEq AND VideoContainerWritingToFileTask.version = @videoContainerWritingToFileTaskVersionEq)",
    params: {
      videoContainerWritingToFileTaskContainerIdEq: videoContainerWritingToFileTaskContainerIdEq,
      videoContainerWritingToFileTaskVersionEq: Spanner.float(videoContainerWritingToFileTaskVersionEq),
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskContainerIdEq: { type: "string" },
      videoContainerWritingToFileTaskVersionEq: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateVideoContainerSyncingTaskStatement(
  videoContainerSyncingTaskContainerIdEq: string,
  videoContainerSyncingTaskVersionEq: number,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE VideoContainerSyncingTask SET executionTimeMs = @setExecutionTimeMs WHERE (VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq AND VideoContainerSyncingTask.version = @videoContainerSyncingTaskVersionEq)",
    params: {
      videoContainerSyncingTaskContainerIdEq: videoContainerSyncingTaskContainerIdEq,
      videoContainerSyncingTaskVersionEq: Spanner.float(videoContainerSyncingTaskVersionEq),
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
      videoContainerSyncingTaskVersionEq: { type: "float64" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateMediaFormattingTaskStatement(
  mediaFormattingTaskContainerIdEq: string,
  mediaFormattingTaskGcsFilenameEq: string,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE MediaFormattingTask SET executionTimeMs = @setExecutionTimeMs WHERE (MediaFormattingTask.containerId = @mediaFormattingTaskContainerIdEq AND MediaFormattingTask.gcsFilename = @mediaFormattingTaskGcsFilenameEq)",
    params: {
      mediaFormattingTaskContainerIdEq: mediaFormattingTaskContainerIdEq,
      mediaFormattingTaskGcsFilenameEq: mediaFormattingTaskGcsFilenameEq,
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      mediaFormattingTaskContainerIdEq: { type: "string" },
      mediaFormattingTaskGcsFilenameEq: { type: "string" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateSubtitleFormattingTaskStatement(
  subtitleFormattingTaskContainerIdEq: string,
  subtitleFormattingTaskGcsFilenameEq: string,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE SubtitleFormattingTask SET executionTimeMs = @setExecutionTimeMs WHERE (SubtitleFormattingTask.containerId = @subtitleFormattingTaskContainerIdEq AND SubtitleFormattingTask.gcsFilename = @subtitleFormattingTaskGcsFilenameEq)",
    params: {
      subtitleFormattingTaskContainerIdEq: subtitleFormattingTaskContainerIdEq,
      subtitleFormattingTaskGcsFilenameEq: subtitleFormattingTaskGcsFilenameEq,
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      subtitleFormattingTaskContainerIdEq: { type: "string" },
      subtitleFormattingTaskGcsFilenameEq: { type: "string" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateGcsFileDeletingTaskStatement(
  gcsFileDeletingTaskFilenameEq: string,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE GcsFileDeletingTask SET executionTimeMs = @setExecutionTimeMs WHERE GcsFileDeletingTask.filename = @gcsFileDeletingTaskFilenameEq",
    params: {
      gcsFileDeletingTaskFilenameEq: gcsFileDeletingTaskFilenameEq,
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      gcsFileDeletingTaskFilenameEq: { type: "string" },
      setExecutionTimeMs: { type: "timestamp" },
    }
  };
}

export function updateR2KeyDeletingTaskStatement(
  r2KeyDeletingTaskKeyEq: string,
  setExecutionTimeMs: number,
): Statement {
  return {
    sql: "UPDATE R2KeyDeletingTask SET executionTimeMs = @setExecutionTimeMs WHERE R2KeyDeletingTask.key = @r2KeyDeletingTaskKeyEq",
    params: {
      r2KeyDeletingTaskKeyEq: r2KeyDeletingTaskKeyEq,
      setExecutionTimeMs: new Date(setExecutionTimeMs).toISOString(),
    },
    types: {
      r2KeyDeletingTaskKeyEq: { type: "string" },
      setExecutionTimeMs: { type: "timestamp" },
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
  videoContainerWritingToFileTaskExecutionTimeMs: number,
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
    name: 'videoContainerWritingToFileTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listVideoContainerWritingToFileTasks(
  runner: Database | Transaction,
  videoContainerWritingToFileTaskExecutionTimeMsLe: number,
): Promise<Array<ListVideoContainerWritingToFileTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerWritingToFileTask.containerId, VideoContainerWritingToFileTask.version, VideoContainerWritingToFileTask.executionTimeMs FROM VideoContainerWritingToFileTask WHERE VideoContainerWritingToFileTask.executionTimeMs <= @videoContainerWritingToFileTaskExecutionTimeMsLe ORDER BY VideoContainerWritingToFileTask.executionTimeMs",
    params: {
      videoContainerWritingToFileTaskExecutionTimeMsLe: new Date(videoContainerWritingToFileTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      videoContainerWritingToFileTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListVideoContainerWritingToFileTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerWritingToFileTaskContainerId: row.at(0).value,
      videoContainerWritingToFileTaskVersion: row.at(1).value.value,
      videoContainerWritingToFileTaskExecutionTimeMs: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListVideoContainerSyncingTasksRow {
  videoContainerSyncingTaskContainerId: string,
  videoContainerSyncingTaskVersion: number,
  videoContainerSyncingTaskExecutionTimeMs: number,
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
    name: 'videoContainerSyncingTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listVideoContainerSyncingTasks(
  runner: Database | Transaction,
  videoContainerSyncingTaskExecutionTimeMsLe: number,
): Promise<Array<ListVideoContainerSyncingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.containerId, VideoContainerSyncingTask.version, VideoContainerSyncingTask.executionTimeMs FROM VideoContainerSyncingTask WHERE VideoContainerSyncingTask.executionTimeMs <= @videoContainerSyncingTaskExecutionTimeMsLe ORDER BY VideoContainerSyncingTask.executionTimeMs",
    params: {
      videoContainerSyncingTaskExecutionTimeMsLe: new Date(videoContainerSyncingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      videoContainerSyncingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListVideoContainerSyncingTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskContainerId: row.at(0).value,
      videoContainerSyncingTaskVersion: row.at(1).value.value,
      videoContainerSyncingTaskExecutionTimeMs: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListMediaFormattingTasksRow {
  mediaFormattingTaskContainerId: string,
  mediaFormattingTaskGcsFilename: string,
  mediaFormattingTaskExecutionTimeMs: number,
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
    name: 'mediaFormattingTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listMediaFormattingTasks(
  runner: Database | Transaction,
  mediaFormattingTaskExecutionTimeMsLe: number,
): Promise<Array<ListMediaFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT MediaFormattingTask.containerId, MediaFormattingTask.gcsFilename, MediaFormattingTask.executionTimeMs FROM MediaFormattingTask WHERE MediaFormattingTask.executionTimeMs <= @mediaFormattingTaskExecutionTimeMsLe ORDER BY MediaFormattingTask.executionTimeMs",
    params: {
      mediaFormattingTaskExecutionTimeMsLe: new Date(mediaFormattingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      mediaFormattingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListMediaFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      mediaFormattingTaskContainerId: row.at(0).value,
      mediaFormattingTaskGcsFilename: row.at(1).value,
      mediaFormattingTaskExecutionTimeMs: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListSubtitleFormattingTasksRow {
  subtitleFormattingTaskContainerId: string,
  subtitleFormattingTaskGcsFilename: string,
  subtitleFormattingTaskExecutionTimeMs: number,
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
    name: 'subtitleFormattingTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listSubtitleFormattingTasks(
  runner: Database | Transaction,
  subtitleFormattingTaskExecutionTimeMsLe: number,
): Promise<Array<ListSubtitleFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.gcsFilename, SubtitleFormattingTask.executionTimeMs FROM SubtitleFormattingTask WHERE SubtitleFormattingTask.executionTimeMs <= @subtitleFormattingTaskExecutionTimeMsLe ORDER BY SubtitleFormattingTask.executionTimeMs",
    params: {
      subtitleFormattingTaskExecutionTimeMsLe: new Date(subtitleFormattingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      subtitleFormattingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListSubtitleFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      subtitleFormattingTaskContainerId: row.at(0).value,
      subtitleFormattingTaskGcsFilename: row.at(1).value,
      subtitleFormattingTaskExecutionTimeMs: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListGcsFileDeletingTasksRow {
  gcsFileDeletingTaskFilename: string,
  gcsFileDeletingTaskUploadSessionUrl: string,
  gcsFileDeletingTaskExecutionTimeMs: number,
}

export let LIST_GCS_FILE_DELETING_TASKS_ROW: MessageDescriptor<ListGcsFileDeletingTasksRow> = {
  name: 'ListGcsFileDeletingTasksRow',
  fields: [{
    name: 'gcsFileDeletingTaskFilename',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskUploadSessionUrl',
    index: 2,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'gcsFileDeletingTaskExecutionTimeMs',
    index: 3,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listGcsFileDeletingTasks(
  runner: Database | Transaction,
  gcsFileDeletingTaskExecutionTimeMsLe: number,
): Promise<Array<ListGcsFileDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT GcsFileDeletingTask.filename, GcsFileDeletingTask.uploadSessionUrl, GcsFileDeletingTask.executionTimeMs FROM GcsFileDeletingTask WHERE GcsFileDeletingTask.executionTimeMs <= @gcsFileDeletingTaskExecutionTimeMsLe ORDER BY GcsFileDeletingTask.executionTimeMs",
    params: {
      gcsFileDeletingTaskExecutionTimeMsLe: new Date(gcsFileDeletingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      gcsFileDeletingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListGcsFileDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      gcsFileDeletingTaskFilename: row.at(0).value,
      gcsFileDeletingTaskUploadSessionUrl: row.at(1).value,
      gcsFileDeletingTaskExecutionTimeMs: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface ListR2KeyDeletingTasksRow {
  r2KeyDeletingTaskKey: string,
  r2KeyDeletingTaskExecutionTimeMs: number,
}

export let LIST_R2_KEY_DELETING_TASKS_ROW: MessageDescriptor<ListR2KeyDeletingTasksRow> = {
  name: 'ListR2KeyDeletingTasksRow',
  fields: [{
    name: 'r2KeyDeletingTaskKey',
    index: 1,
    primitiveType: PrimitiveType.STRING,
  }, {
    name: 'r2KeyDeletingTaskExecutionTimeMs',
    index: 2,
    primitiveType: PrimitiveType.NUMBER,
  }],
};

export async function listR2KeyDeletingTasks(
  runner: Database | Transaction,
  r2KeyDeletingTaskExecutionTimeMsLe: number,
): Promise<Array<ListR2KeyDeletingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT R2KeyDeletingTask.key, R2KeyDeletingTask.executionTimeMs FROM R2KeyDeletingTask WHERE R2KeyDeletingTask.executionTimeMs <= @r2KeyDeletingTaskExecutionTimeMsLe ORDER BY R2KeyDeletingTask.executionTimeMs",
    params: {
      r2KeyDeletingTaskExecutionTimeMsLe: new Date(r2KeyDeletingTaskExecutionTimeMsLe).toISOString(),
    },
    types: {
      r2KeyDeletingTaskExecutionTimeMsLe: { type: "timestamp" },
    }
  });
  let resRows = new Array<ListR2KeyDeletingTasksRow>();
  for (let row of rows) {
    resRows.push({
      r2KeyDeletingTaskKey: row.at(0).value,
      r2KeyDeletingTaskExecutionTimeMs: row.at(1).value.valueOf(),
    });
  }
  return resRows;
}
