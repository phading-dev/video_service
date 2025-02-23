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
import { DropSubtitleTrackStagingDataHandler } from "./drop_subtitle_track_staging_data_handler";
import { newNotFoundError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteStorageEndRecordingTaskStatement("root/subtitleTrack1"),
      deleteStorageEndRecordingTaskStatement("root/subtitleTrack2"),
      deleteStorageEndRecordingTaskStatement("root/subtitleTrack3"),
      deleteR2KeyDeletingTaskStatement("root/subtitleTrack1"),
      deleteR2KeyDeletingTaskStatement("root/subtitleTrack2"),
      deleteR2KeyDeletingTaskStatement("root/subtitleTrack3"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "DropSubtitleTrackStagingDataHandlerTest",
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
              r2RootDirname: "root",
              subtitleTracks: [
                {
                  r2TrackDirname: "subtitleTrack1",
                  staging: {
                    toAdd: {
                      name: "name1",
                      isDefault: true,
                      totalBytes: 100,
                    },
                  },
                },
                {
                  r2TrackDirname: "subtitleTrack2",
                  staging: {
                    toAdd: {
                      name: "name2",
                      isDefault: false,
                      totalBytes: 100,
                    },
                  },
                },
                {
                  r2TrackDirname: "subtitleTrack3",
                  committed: {
                    name: "name3",
                    isDefault: false,
                    totalBytes: 100,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropSubtitleTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "subtitleTrack2",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  containerId: "container1",
                  accountId: "account1",
                  r2RootDirname: "root",
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitleTrack1",
                      staging: {
                        toAdd: {
                          name: "name1",
                          isDefault: true,
                          totalBytes: 100,
                        },
                      },
                    },
                    {
                      r2TrackDirname: "subtitleTrack3",
                      committed: {
                        name: "name3",
                        isDefault: false,
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
          await getStorageEndRecordingTask(
            SPANNER_DATABASE,
            "root/subtitleTrack2",
          ),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/subtitleTrack2",
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
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/subtitleTrack2"),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/subtitleTrack2",
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
              r2RootDirname: "root",
              subtitleTracks: [
                {
                  r2TrackDirname: "subtitleTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                  staging: {
                    toAdd: {
                      name: "newName1",
                      isDefault: false,
                      totalBytes: 100,
                    },
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropSubtitleTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "subtitleTrack1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  containerId: "container1",
                  accountId: "account1",
                  r2RootDirname: "root",
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitleTrack1",
                      committed: {
                        name: "name1",
                        isDefault: true,
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
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, 10000000),
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
              r2RootDirname: "root",
              subtitleTracks: [
                {
                  r2TrackDirname: "subtitleTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "subtitleTrack2",
                  committed: {
                    name: "name2",
                    isDefault: false,
                    totalBytes: 100,
                  },
                  staging: {
                    toDelete: true,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropSubtitleTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
          r2TrackDirname: "subtitleTrack2",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  containerId: "container1",
                  accountId: "account1",
                  r2RootDirname: "root",
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitleTrack1",
                      committed: {
                        name: "name1",
                        isDefault: true,
                        totalBytes: 100,
                      },
                    },
                    {
                      r2TrackDirname: "subtitleTrack2",
                      committed: {
                        name: "name2",
                        isDefault: false,
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
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, 10000000),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "SubtitleTrackNotFound",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              r2RootDirname: "root",
              subtitleTracks: [
                {
                  r2TrackDirname: "subtitleTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "subtitleTrack3",
                  committed: {
                    name: "name3",
                    isDefault: false,
                    totalBytes: 100,
                  },
                  staging: {
                    toDelete: true,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DropSubtitleTrackStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            r2TrackDirname: "subtitleTrack2",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newNotFoundError("subtitle track subtitleTrack2 is not found"),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
