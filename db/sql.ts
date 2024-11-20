import { VideoContainerData, VIDEO_CONTAINER_DATA, VideoTrackData, VIDEO_TRACK_DATA, AudioTrackData, AUDIO_TRACK_DATA, SubtitleTrackData, SUBTITLE_TRACK_DATA } from './schema';
import { deserializeMessage, serializeMessage } from '@selfage/message/serializer';
import { Database, Transaction } from '@google-cloud/spanner';
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

export function updateVideoFormattingTaskStatement(
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
