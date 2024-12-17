import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_GCS_FILE_DELETE_TASKS_ROW,
  GET_R2_KEY_DELETE_TASKS_ROW,
  deleteGcsFileDeleteTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyDeleteTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getGcsFileDeleteTasks,
  getR2KeyDeleteTasks,
  getVideoContainer,
  getVideoContainerSyncingTasks,
  getVideoContainerWritingToFileTasks,
  insertMediaFormattingTaskStatement,
  insertSubtitleFormattingTaskStatement,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  insertVideoContainerWritingToFileTaskStatement,
} from "../db/sql";
import { DeleteVideoContainerHandler } from "./delete_video_container_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray, isUnorderedArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteVideoContainerWritingToFileTaskStatement("container1", 1),
      deleteVideoContainerSyncingTaskStatement("container1", 1),
      deleteMediaFormattingTaskStatement("container1", "media1"),
      deleteSubtitleFormattingTaskStatement("container1", "subtitle1"),
      deleteGcsFileDeleteTaskStatement("media1"),
      deleteGcsFileDeleteTaskStatement("subtitle1"),
      deleteR2KeyDeleteTaskStatement("root/master0"),
      deleteR2KeyDeleteTaskStatement("root/master1"),
      deleteR2KeyDeleteTaskStatement("root/video1"),
      deleteR2KeyDeleteTaskStatement("root/video2"),
      deleteR2KeyDeleteTaskStatement("root/audio1"),
      deleteR2KeyDeleteTaskStatement("root/audio2"),
      deleteR2KeyDeleteTaskStatement("root/subtitle1"),
      deleteR2KeyDeleteTaskStatement("root/subtitle2"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "DeleteVideoContainerHandlerTest",
  cases: [
    {
      name: "Delete",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                synced: {
                  version: 1,
                  r2Filename: "master1",
                },
              },
              videoTracks: [
                {
                  r2TrackDirname: "video1",
                  committed: {
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 1000,
                  },
                },
                {
                  r2TrackDirname: "video2",
                  staging: {
                    toAdd: {
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 1000,
                    },
                  },
                },
              ],
              audioTracks: [
                {
                  r2TrackDirname: "audio1",
                  committed: {
                    name: "audio1",
                    isDefault: true,
                    totalBytes: 1000,
                  },
                },
                {
                  r2TrackDirname: "audio2",
                  staging: {
                    toAdd: {
                      name: "audio2",
                      isDefault: false,
                      totalBytes: 1000,
                    },
                  },
                },
              ],
              subtitleTracks: [
                {
                  r2TrackDirname: "subtitle1",
                  committed: {
                    name: "subtitle1",
                    isDefault: true,
                    totalBytes: 1000,
                  },
                },
                {
                  r2TrackDirname: "subtitle2",
                  staging: {
                    toAdd: {
                      name: "subtitle2",
                      isDefault: false,
                      totalBytes: 1000,
                    },
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/master1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/video1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/video2",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/audio1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/audio2",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/subtitle1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/subtitle2",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
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
      name: "DeleteWritingToFile",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                writingToFile: {
                  version: 1,
                  r2FilenamesToDelete: ["master0"],
                  r2DirnamesToDelete: ["video1", "audio1"],
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
            insertVideoContainerWritingToFileTaskStatement(
              "container1",
              1,
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getVideoContainerWritingToFileTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/master0",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/video1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/audio1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
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
      name: "DeleteSyncing",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                syncing: {
                  version: 1,
                  r2Filename: "master1",
                  r2FilenamesToDelete: ["master0"],
                  r2DirnamesToDelete: ["video1", "audio1"],
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
            insertVideoContainerSyncingTaskStatement("container1", 1, 0, 0),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "syncing tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/master0",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/master1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/video1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/audio1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
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
      name: "DeleteMediaUploading",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                synced: {
                  version: 1,
                  r2Filename: "master1",
                },
              },
              processing: {
                media: {
                  uploading: {
                    gcsFilename: "media1",
                    uploadSessionUrl: "uploadUrl1",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "media1",
                gcsFileDeleteTaskPayload: {
                  uploadSessionUrl: "uploadUrl1",
                },
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DeleteMediaFormatting",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                synced: {
                  version: 1,
                  r2Filename: "master1",
                },
              },
              processing: {
                media: {
                  formatting: {
                    gcsFilename: "media1",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
            insertMediaFormattingTaskStatement("container1", "media1", 0, 0),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "media1",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DeleteSubtitleUploading",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                synced: {
                  version: 1,
                  r2Filename: "master1",
                },
              },
              processing: {
                subtitle: {
                  uploading: {
                    gcsFilename: "subtitle1",
                    uploadSessionUrl: "uploadUrl1",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "subtitle1",
                gcsFileDeleteTaskPayload: {
                  uploadSessionUrl: "uploadUrl1",
                },
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "DeleteSubtitleFormatting",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              masterPlaylist: {
                synced: {
                  version: 1,
                  r2Filename: "master1",
                },
              },
              processing: {
                subtitle: {
                  formatting: {
                    gcsFilename: "subtitle1",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
              subtitleTracks: [],
            }),
            insertSubtitleFormattingTaskStatement(
              "container1",
              "subtitle1",
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([]),
          "video container",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "subtitle1",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "AlreadyDeleted",
      execute: async () => {
        // Prepare
        let handler = new DeleteVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("", {
          containerId: "container1",
        });
        // Nothing happens.
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
