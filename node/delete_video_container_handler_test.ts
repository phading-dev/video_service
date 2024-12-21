import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  LIST_GCS_FILE_DELETING_TASKS_ROW,
  LIST_R2_KEY_DELETING_TASKS_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertSubtitleFormattingTaskStatement,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  insertVideoContainerWritingToFileTaskStatement,
  listGcsFileDeletingTasks,
  listR2KeyDeletingTasks,
  listVideoContainerSyncingTasks,
  listVideoContainerWritingToFileTasks,
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
      deleteGcsFileDeletingTaskStatement("media1"),
      deleteGcsFileDeletingTaskStatement("subtitle1"),
      deleteR2KeyDeletingTaskStatement("root/master0"),
      deleteR2KeyDeletingTaskStatement("root/master1"),
      deleteR2KeyDeletingTaskStatement("root/video1"),
      deleteR2KeyDeletingTaskStatement("root/video2"),
      deleteR2KeyDeletingTaskStatement("root/audio1"),
      deleteR2KeyDeletingTaskStatement("root/audio2"),
      deleteR2KeyDeletingTaskStatement("root/subtitle1"),
      deleteR2KeyDeletingTaskStatement("root/subtitle2"),
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
          await listR2KeyDeletingTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/master1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/video1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/video2",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audio1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audio2",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/subtitle1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/subtitle2",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
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
          await listVideoContainerWritingToFileTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/master0",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/video1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audio1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
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
          await listVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "syncing tasks",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/master0",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/master1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/video1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audio1",
                r2KeyDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
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
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "media1",
                gcsFileDeletingTaskPayload: {
                  uploadSessionUrl: "uploadUrl1",
                },
                gcsFileDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
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
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "media1",
                gcsFileDeletingTaskPayload: {},
                gcsFileDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
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
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "subtitle1",
                gcsFileDeletingTaskPayload: {
                  uploadSessionUrl: "uploadUrl1",
                },
                gcsFileDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
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
          await listGcsFileDeletingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "subtitle1",
                gcsFileDeletingTaskPayload: {},
                gcsFileDeletingTaskExecutionTimestamp: 1000,
              },
              LIST_GCS_FILE_DELETING_TASKS_ROW,
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
