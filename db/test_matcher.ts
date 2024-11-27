import {
  GetGcsFileCleanupTasksRow,
  GetMediaFormattingTasksRow,
  GetR2KeyCleanupTasksRow,
  GetVideoContainerSyncingTasksRow,
} from "./sql";
import { MatchFn, assertThat, eq } from "@selfage/test_matcher";

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

export function eqGetMediaFormattingTasksRow(
  expected: GetMediaFormattingTasksRow,
): MatchFn<GetMediaFormattingTasksRow> {
  return (actual: GetMediaFormattingTasksRow) => {
    assertThat(
      actual.mediaFormattingTaskContainerId,
      eq(expected.mediaFormattingTaskContainerId),
      "containerId",
    );
    assertThat(
      actual.mediaFormattingTaskGcsFilename,
      eq(expected.mediaFormattingTaskGcsFilename),
      "gcsFilename",
    );
    assertThat(
      actual.mediaFormattingTaskExecutionTimestamp,
      eq(expected.mediaFormattingTaskExecutionTimestamp),
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
