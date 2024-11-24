import { AUDIO_TRACK_DATA, VIDEO_TRACK_DATA } from "./schema";
import {
  GetAllAudioTracksRow,
  GetAllVideoTracksRow,
  GetGcsFileCleanupTasksRow,
  GetR2KeyCleanupTasksRow,
  GetVideoContainerSyncingTasksRow,
  GetVideoFormattingTasksRow,
} from "./sql";
import { eqMessage } from "@selfage/message/test_matcher";
import { MatchFn, assertThat, eq } from "@selfage/test_matcher";

export function eqGetAllVideoTracksRow(
  expected: GetAllVideoTracksRow,
): MatchFn<GetAllVideoTracksRow> {
  return (actual: GetAllVideoTracksRow) => {
    assertThat(
      actual.videoTrackVideoId,
      eq(expected.videoTrackVideoId),
      "videoId",
    );
    assertThat(
      actual.videoTrackData,
      eqMessage(expected.videoTrackData, VIDEO_TRACK_DATA),
      "data",
    );
  };
}

export function eqGetAllAudioTracksRow(
  expected: GetAllAudioTracksRow,
): MatchFn<GetAllAudioTracksRow> {
  return (actual: GetAllAudioTracksRow) => {
    assertThat(
      actual.audioTrackAudioId,
      eq(expected.audioTrackAudioId),
      "audioId",
    );
    assertThat(
      actual.audioTrackData,
      eqMessage(expected.audioTrackData, AUDIO_TRACK_DATA),
      "data",
    );
  };
}

export function eqGetVideoContainerSyncingTasksRow(
  expected: GetVideoContainerSyncingTasksRow,
): MatchFn<GetVideoContainerSyncingTasksRow> {
  return (actual: GetVideoContainerSyncingTasksRow) => {
    assertThat(
      actual.videoContainerSyncingTaskContainerId,
      eq(expected.videoContainerSyncingTaskContainerId),
      "containerId",
    );
    assertThat(
      actual.videoContainerSyncingTaskVersion,
      eq(expected.videoContainerSyncingTaskVersion),
      "videoId",
    );
    assertThat(
      actual.videoContainerSyncingTaskExecutionTimestamp,
      eq(expected.videoContainerSyncingTaskExecutionTimestamp),
      "executionTimestamp",
    );
  };
}

export function eqGetVideoFormattingTasksRow(
  expected: GetVideoFormattingTasksRow,
): MatchFn<GetVideoFormattingTasksRow> {
  return (actual: GetVideoFormattingTasksRow) => {
    assertThat(
      actual.videoFormattingTaskContainerId,
      eq(expected.videoFormattingTaskContainerId),
      "containerId",
    );
    assertThat(
      actual.videoFormattingTaskVideoId,
      eq(expected.videoFormattingTaskVideoId),
      "videoId",
    );
    assertThat(
      actual.videoFormattingTaskExecutionTimestamp,
      eq(expected.videoFormattingTaskExecutionTimestamp),
      "executionTimestamp",
    );
  };
}

export function eqGetGcsFileCleanupTasksRow(
  expected: GetGcsFileCleanupTasksRow,
): MatchFn<GetGcsFileCleanupTasksRow> {
  return (actual: GetGcsFileCleanupTasksRow) => {
    assertThat(
      actual.gcsFileCleanupTaskFilename,
      eq(expected.gcsFileCleanupTaskFilename),
      "filename",
    );
    assertThat(
      actual.gcsFileCleanupTaskExecutionTimestamp,
      eq(expected.gcsFileCleanupTaskExecutionTimestamp),
      "executionTimestamp",
    );
  };
}

export function eqGetR2KeyCleanupTasksRow(
  expected: GetR2KeyCleanupTasksRow,
): MatchFn<GetR2KeyCleanupTasksRow> {
  return (actual: GetR2KeyCleanupTasksRow) => {
    assertThat(
      actual.r2KeyCleanupTaskKey,
      eq(expected.r2KeyCleanupTaskKey),
      "key",
    );
    assertThat(
      actual.r2KeyCleanupTaskExecutionTimestamp,
      eq(expected.r2KeyCleanupTaskExecutionTimestamp),
      "executionTimestamp",
    );
  };
}
