import { VideoContainerData, VIDEO_CONTAINER_DATA, VideoTrackData, VIDEO_TRACK_DATA, AudioTrackData, AUDIO_TRACK_DATA, SubtitleTrackData, SUBTITLE_TRACK_DATA } from './schema';
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

export interface GetAllVideoTracksRow {
  videoTrackVideoId: string,
  videoTrackData: VideoTrackData,
}

export async function getAllVideoTracks(
  runner: Database | Transaction,
  videoTrackContainerIdEq: string,
): Promise<Array<GetAllVideoTracksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoTrack.videoId, VideoTrack.data FROM VideoTrack WHERE VideoTrack.containerId = @videoTrackContainerIdEq",
    params: {
      videoTrackContainerIdEq: videoTrackContainerIdEq,
    },
    types: {
      videoTrackContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetAllVideoTracksRow>();
  for (let row of rows) {
    resRows.push({
      videoTrackVideoId: row.at(0).value,
      videoTrackData: deserializeMessage(row.at(1).value, VIDEO_TRACK_DATA),
    });
  }
  return resRows;
}

export interface GetVideoTrackRow {
  videoTrackData: VideoTrackData,
}

export async function getVideoTrack(
  runner: Database | Transaction,
  videoTrackContainerIdEq: string,
  videoTrackVideoIdEq: string,
): Promise<Array<GetVideoTrackRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoTrack.data FROM VideoTrack WHERE (VideoTrack.containerId = @videoTrackContainerIdEq AND VideoTrack.videoId = @videoTrackVideoIdEq)",
    params: {
      videoTrackContainerIdEq: videoTrackContainerIdEq,
      videoTrackVideoIdEq: videoTrackVideoIdEq,
    },
    types: {
      videoTrackContainerIdEq: { type: "string" },
      videoTrackVideoIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetVideoTrackRow>();
  for (let row of rows) {
    resRows.push({
      videoTrackData: deserializeMessage(row.at(0).value, VIDEO_TRACK_DATA),
    });
  }
  return resRows;
}

export interface GetAllAudioTracksRow {
  audioTrackAudioId: string,
  audioTrackData: AudioTrackData,
}

export async function getAllAudioTracks(
  runner: Database | Transaction,
  audioTrackContainerIdEq: string,
): Promise<Array<GetAllAudioTracksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT AudioTrack.audioId, AudioTrack.data FROM AudioTrack WHERE AudioTrack.containerId = @audioTrackContainerIdEq",
    params: {
      audioTrackContainerIdEq: audioTrackContainerIdEq,
    },
    types: {
      audioTrackContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetAllAudioTracksRow>();
  for (let row of rows) {
    resRows.push({
      audioTrackAudioId: row.at(0).value,
      audioTrackData: deserializeMessage(row.at(1).value, AUDIO_TRACK_DATA),
    });
  }
  return resRows;
}

export interface GetAudioTrackRow {
  audioTrackData: AudioTrackData,
}

export async function getAudioTrack(
  runner: Database | Transaction,
  audioTrackContainerIdEq: string,
  audioTrackAudioIdEq: string,
): Promise<Array<GetAudioTrackRow>> {
  let [rows] = await runner.run({
    sql: "SELECT AudioTrack.data FROM AudioTrack WHERE (AudioTrack.containerId = @audioTrackContainerIdEq AND AudioTrack.audioId = @audioTrackAudioIdEq)",
    params: {
      audioTrackContainerIdEq: audioTrackContainerIdEq,
      audioTrackAudioIdEq: audioTrackAudioIdEq,
    },
    types: {
      audioTrackContainerIdEq: { type: "string" },
      audioTrackAudioIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetAudioTrackRow>();
  for (let row of rows) {
    resRows.push({
      audioTrackData: deserializeMessage(row.at(0).value, AUDIO_TRACK_DATA),
    });
  }
  return resRows;
}

export interface GetAllSubtitleTracksRow {
  subtitleTrackSubtitleId: string,
  subtitleTrackData: SubtitleTrackData,
}

export async function getAllSubtitleTracks(
  runner: Database | Transaction,
  subtitleTrackContainerIdEq: string,
): Promise<Array<GetAllSubtitleTracksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleTrack.subtitleId, SubtitleTrack.data FROM SubtitleTrack WHERE SubtitleTrack.containerId = @subtitleTrackContainerIdEq",
    params: {
      subtitleTrackContainerIdEq: subtitleTrackContainerIdEq,
    },
    types: {
      subtitleTrackContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<GetAllSubtitleTracksRow>();
  for (let row of rows) {
    resRows.push({
      subtitleTrackSubtitleId: row.at(0).value,
      subtitleTrackData: deserializeMessage(row.at(1).value, SUBTITLE_TRACK_DATA),
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

export interface CheckVideoContainerSyncingTaskRow {
  videoContainerSyncingTaskVersion: number,
}

export async function checkVideoContainerSyncingTask(
  runner: Database | Transaction,
  videoContainerSyncingTaskContainerIdEq: string,
): Promise<Array<CheckVideoContainerSyncingTaskRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoContainerSyncingTask.version FROM VideoContainerSyncingTask WHERE VideoContainerSyncingTask.containerId = @videoContainerSyncingTaskContainerIdEq",
    params: {
      videoContainerSyncingTaskContainerIdEq: videoContainerSyncingTaskContainerIdEq,
    },
    types: {
      videoContainerSyncingTaskContainerIdEq: { type: "string" },
    }
  });
  let resRows = new Array<CheckVideoContainerSyncingTaskRow>();
  for (let row of rows) {
    resRows.push({
      videoContainerSyncingTaskVersion: row.at(0).value.value,
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

export interface GetVideoFormattingTasksRow {
  videoFormattingTaskContainerId: string,
  videoFormattingTaskVideoId: string,
  videoFormattingTaskExecutionTimestamp: number,
}

export async function getVideoFormattingTasks(
  runner: Database | Transaction,
  videoFormattingTaskExecutionTimestampLt: number,
): Promise<Array<GetVideoFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT VideoFormattingTask.containerId, VideoFormattingTask.videoId, VideoFormattingTask.executionTimestamp FROM VideoFormattingTask WHERE VideoFormattingTask.executionTimestamp < @videoFormattingTaskExecutionTimestampLt ORDER BY VideoFormattingTask.executionTimestamp DESC",
    params: {
      videoFormattingTaskExecutionTimestampLt: new Date(videoFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      videoFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetVideoFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      videoFormattingTaskContainerId: row.at(0).value,
      videoFormattingTaskVideoId: row.at(1).value,
      videoFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetAudioFormattingTasksRow {
  audioFormattingTaskContainerId: string,
  audioFormattingTaskAudioId: string,
  audioFormattingTaskExecutionTimestamp: number,
}

export async function getAudioFormattingTasks(
  runner: Database | Transaction,
  audioFormattingTaskExecutionTimestampLt: number,
): Promise<Array<GetAudioFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT AudioFormattingTask.containerId, AudioFormattingTask.audioId, AudioFormattingTask.executionTimestamp FROM AudioFormattingTask WHERE AudioFormattingTask.executionTimestamp < @audioFormattingTaskExecutionTimestampLt ORDER BY AudioFormattingTask.executionTimestamp DESC",
    params: {
      audioFormattingTaskExecutionTimestampLt: new Date(audioFormattingTaskExecutionTimestampLt).toISOString(),
    },
    types: {
      audioFormattingTaskExecutionTimestampLt: { type: "timestamp" },
    }
  });
  let resRows = new Array<GetAudioFormattingTasksRow>();
  for (let row of rows) {
    resRows.push({
      audioFormattingTaskContainerId: row.at(0).value,
      audioFormattingTaskAudioId: row.at(1).value,
      audioFormattingTaskExecutionTimestamp: row.at(2).value.valueOf(),
    });
  }
  return resRows;
}

export interface GetSubtitleFormattingTasksRow {
  subtitleFormattingTaskContainerId: string,
  subtitleFormattingTaskSubtitleId: string,
  subtitleFormattingTaskExecutionTimestamp: number,
}

export async function getSubtitleFormattingTasks(
  runner: Database | Transaction,
  subtitleFormattingTaskExecutionTimestampLt: number,
): Promise<Array<GetSubtitleFormattingTasksRow>> {
  let [rows] = await runner.run({
    sql: "SELECT SubtitleFormattingTask.containerId, SubtitleFormattingTask.subtitleId, SubtitleFormattingTask.executionTimestamp FROM SubtitleFormattingTask WHERE SubtitleFormattingTask.executionTimestamp < @subtitleFormattingTaskExecutionTimestampLt ORDER BY SubtitleFormattingTask.executionTimestamp DESC",
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
      subtitleFormattingTaskSubtitleId: row.at(1).value,
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

export function insertVideoTrackStatement(
  containerId: string,
  videoId: string,
  data: VideoTrackData,
): Statement {
  return {
    sql: "INSERT VideoTrack (containerId, videoId, data) VALUES (@containerId, @videoId, @data)",
    params: {
      containerId: containerId,
      videoId: videoId,
      data: Buffer.from(serializeMessage(data, VIDEO_TRACK_DATA).buffer),
    },
    types: {
      containerId: { type: "string" },
      videoId: { type: "string" },
      data: { type: "bytes" },
    }
  };
}

export function insertAudioTrackStatement(
  containerId: string,
  audioId: string,
  data: AudioTrackData,
): Statement {
  return {
    sql: "INSERT AudioTrack (containerId, audioId, data) VALUES (@containerId, @audioId, @data)",
    params: {
      containerId: containerId,
      audioId: audioId,
      data: Buffer.from(serializeMessage(data, AUDIO_TRACK_DATA).buffer),
    },
    types: {
      containerId: { type: "string" },
      audioId: { type: "string" },
      data: { type: "bytes" },
    }
  };
}

export function insertSubtitleTrackStatement(
  containerId: string,
  subtitleId: string,
  data: SubtitleTrackData,
): Statement {
  return {
    sql: "INSERT SubtitleTrack (containerId, subtitleId, data) VALUES (@containerId, @subtitleId, @data)",
    params: {
      containerId: containerId,
      subtitleId: subtitleId,
      data: Buffer.from(serializeMessage(data, SUBTITLE_TRACK_DATA).buffer),
    },
    types: {
      containerId: { type: "string" },
      subtitleId: { type: "string" },
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

export function insertVideoFormattingTaskStatement(
  containerId: string,
  videoId: string,
  executionTimestamp: number,
  createdTimestamp: number,
): Statement {
  return {
    sql: "INSERT VideoFormattingTask (containerId, videoId, executionTimestamp, createdTimestamp) VALUES (@containerId, @videoId, @executionTimestamp, @createdTimestamp)",
    params: {
      containerId: containerId,
      videoId: videoId,
      executionTimestamp: new Date(executionTimestamp).toISOString(),
      createdTimestamp: new Date(createdTimestamp).toISOString(),
    },
    types: {
      containerId: { type: "string" },
      videoId: { type: "string" },
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

export function updateVideoTrackStatement(
  setData: VideoTrackData,
  videoTrackContainerIdEq: string,
  videoTrackVideoIdEq: string,
): Statement {
  return {
    sql: "UPDATE VideoTrack SET data = @setData WHERE (VideoTrack.containerId = @videoTrackContainerIdEq AND VideoTrack.videoId = @videoTrackVideoIdEq)",
    params: {
      setData: Buffer.from(serializeMessage(setData, VIDEO_TRACK_DATA).buffer),
      videoTrackContainerIdEq: videoTrackContainerIdEq,
      videoTrackVideoIdEq: videoTrackVideoIdEq,
    },
    types: {
      setData: { type: "bytes" },
      videoTrackContainerIdEq: { type: "string" },
      videoTrackVideoIdEq: { type: "string" },
    }
  };
}

export function delayVideoFormattingTaskStatement(
  setExecutionTimestamp: number,
  videoFormattingTaskContainerIdEq: string,
  videoFormattingTaskVideoIdEq: string,
): Statement {
  return {
    sql: "UPDATE VideoFormattingTask SET executionTimestamp = @setExecutionTimestamp WHERE (VideoFormattingTask.containerId = @videoFormattingTaskContainerIdEq AND VideoFormattingTask.videoId = @videoFormattingTaskVideoIdEq)",
    params: {
      setExecutionTimestamp: new Date(setExecutionTimestamp).toISOString(),
      videoFormattingTaskContainerIdEq: videoFormattingTaskContainerIdEq,
      videoFormattingTaskVideoIdEq: videoFormattingTaskVideoIdEq,
    },
    types: {
      setExecutionTimestamp: { type: "timestamp" },
      videoFormattingTaskContainerIdEq: { type: "string" },
      videoFormattingTaskVideoIdEq: { type: "string" },
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

export function deleteVideoTrackStatement(
  videoTrackContainerIdEq: string,
  videoTrackVideoIdEq: string,
): Statement {
  return {
    sql: "DELETE VideoTrack WHERE (VideoTrack.containerId = @videoTrackContainerIdEq AND VideoTrack.videoId = @videoTrackVideoIdEq)",
    params: {
      videoTrackContainerIdEq: videoTrackContainerIdEq,
      videoTrackVideoIdEq: videoTrackVideoIdEq,
    },
    types: {
      videoTrackContainerIdEq: { type: "string" },
      videoTrackVideoIdEq: { type: "string" },
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

export function deleteVideoFormattingTaskStatement(
  videoFormattingTaskContainerIdEq: string,
  videoFormattingTaskVideoIdEq: string,
): Statement {
  return {
    sql: "DELETE VideoFormattingTask WHERE (VideoFormattingTask.containerId = @videoFormattingTaskContainerIdEq AND VideoFormattingTask.videoId = @videoFormattingTaskVideoIdEq)",
    params: {
      videoFormattingTaskContainerIdEq: videoFormattingTaskContainerIdEq,
      videoFormattingTaskVideoIdEq: videoFormattingTaskVideoIdEq,
    },
    types: {
      videoFormattingTaskContainerIdEq: { type: "string" },
      videoFormattingTaskVideoIdEq: { type: "string" },
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
