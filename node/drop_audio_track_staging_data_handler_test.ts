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
import { DropAudioTrackStagingDataHandler } from "./drop_audio_track_staging_data_handler";
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
        storageEndRecordingTaskR2DirnameEq: "root/audioTrack1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/audioTrack2",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/audioTrack3",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/audioTrack1",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/audioTrack2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/audioTrack3",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "DropAudioTrackStagingDataHandlerTest",
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
                audioTracks: [
                  {
                    r2TrackDirname: "audioTrack1",
                    totalBytes: 100,
                    staging: {
                      toAdd: {
                        name: "name1",
                        isDefault: true,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack2",
                    totalBytes: 100,
                    staging: {
                      toAdd: {
                        name: "name2",
                        isDefault: false,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack3",
                    totalBytes: 100,
                    committed: {
                      name: "name3",
                      isDefault: false,
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropAudioTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "audioTrack2",
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
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      totalBytes: 100,
                      staging: {
                        toAdd: {
                          name: "name1",
                          isDefault: true,
                        },
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack3",
                      totalBytes: 100,
                      committed: {
                        name: "name3",
                        isDefault: false,
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
            storageEndRecordingTaskR2DirnameEq: "root/audioTrack2",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/audioTrack2",
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
            r2KeyDeletingTaskKeyEq: "root/audioTrack2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audioTrack2",
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
      name: "DropToUpdate",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                audioTracks: [
                  {
                    r2TrackDirname: "audioTrack1",
                    totalBytes: 100,
                    committed: {
                      name: "name1",
                      isDefault: true,
                    },
                    staging: {
                      toAdd: {
                        name: "newName1",
                        isDefault: false,
                      },
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropAudioTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "audioTrack1",
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
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      totalBytes: 100,
                      committed: {
                        name: "name1",
                        isDefault: true,
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
                audioTracks: [
                  {
                    r2TrackDirname: "audioTrack1",
                    totalBytes: 100,
                    committed: {
                      name: "name1",
                      isDefault: true,
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack2",
                    totalBytes: 100,
                    committed: {
                      name: "name2",
                      isDefault: false,
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
        let handler = new DropAudioTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "audioTrack2",
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
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      totalBytes: 100,
                      committed: {
                        name: "name1",
                        isDefault: true,
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack2",
                      totalBytes: 100,
                      committed: {
                        name: "name2",
                        isDefault: false,
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
      name: "AudioTrackNotFound",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                r2RootDirname: "root",
                audioTracks: [
                  {
                    r2TrackDirname: "audioTrack1",
                    totalBytes: 100,
                    committed: {
                      name: "name1",
                      isDefault: true,
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack3",
                    totalBytes: 100,
                    committed: {
                      name: "name3",
                      isDefault: false,
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
        let handler = new DropAudioTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            r2TrackDirname: "audioTrack2",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newNotFoundError("audio track audioTrack2 is not found")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
