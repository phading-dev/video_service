import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_END_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteR2KeyDeletingTaskStatement,
  deleteStorageEndRecordingTaskStatement,
  deleteVideoContainerStatement,
  getR2KeyDeletingTask,
  getStorageEndRecordingTask,
  getVideoContainer,
  insertVideoContainerStatement,
  listPendingR2KeyDeletingTasks,
  listPendingStorageEndRecordingTasks,
} from "../db/sql";
import { DropVideoTrackStagingDataHandler } from "./drop_video_track_staging_data_handler";
import { newNotFoundError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/videoTrack1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/videoTrack2",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/videoTrack3",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/videoTrack1",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/videoTrack2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/videoTrack3",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "DropVideoTrackStagingDataHandlerTest",
  cases: [
    {
      name: "DropToAdd",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                videoTracks: [
                  {
                    r2TrackDirname: "videoTrack1",
                    staging: {
                      toAdd: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 100,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack2",
                    staging: {
                      toAdd: {
                        durationSec: 120,
                        resolution: "1280x720",
                        totalBytes: 100,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack3",
                    committed: {
                      durationSec: 180,
                      resolution: "640x360",
                      totalBytes: 100,
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropVideoTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "videoTrack2",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData: {
                  r2RootDirname: "root",
                  videoTracks: [
                    {
                      r2TrackDirname: "videoTrack1",
                      staging: {
                        toAdd: {
                          durationSec: 60,
                          resolution: "1920x1080",
                          totalBytes: 100,
                        },
                      },
                    },
                    {
                      r2TrackDirname: "videoTrack3",
                      committed: {
                        durationSec: 180,
                        resolution: "640x360",
                        totalBytes: 100,
                      },
                    },
                  ],
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/videoTrack2",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/videoTrack2",
                storageEndRecordingTaskPayload: {
                  accountId: "account1",
                  endTimeMs: 1000,
                },
                storageEndRecordingTaskRetryCount: 0,
                storageEndRecordingTaskExecutionTimeMs: 1000,
                storageEndRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_END_RECORDING_TASK_ROW,
            ),
          ]),
          "storage end recording tasks",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/videoTrack2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/videoTrack2",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DropToDelete",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                videoTracks: [
                  {
                    r2TrackDirname: "videoTrack1",
                    staging: {
                      toAdd: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 100,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack2",
                    committed: {
                      durationSec: 120,
                      resolution: "1280x720",
                      totalBytes: 100,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropVideoTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "videoTrack2",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData: {
                  r2RootDirname: "root",
                  videoTracks: [
                    {
                      r2TrackDirname: "videoTrack1",
                      staging: {
                        toAdd: {
                          durationSec: 60,
                          resolution: "1920x1080",
                          totalBytes: 100,
                        },
                      },
                    },
                    {
                      r2TrackDirname: "videoTrack2",
                      committed: {
                        durationSec: 120,
                        resolution: "1280x720",
                        totalBytes: 100,
                      },
                    },
                  ],
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, {
            storageEndRecordingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "VideoTrackNotFound",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                videoTracks: [
                  {
                    r2TrackDirname: "videoTrack1",
                    committed: {
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 100,
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack3",
                    committed: {
                      durationSec: 180,
                      resolution: "640x360",
                      totalBytes: 100,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropVideoTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            r2TrackDirname: "videoTrack2",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newNotFoundError("video track videoTrack2 is not found")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
