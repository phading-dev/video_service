import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_R2_KEY_DELETE_TASKS_ROW,
  deleteR2KeyDeleteTaskStatement,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
  listR2KeyDeleteTasks,
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
      deleteR2KeyDeleteTaskStatement("root/subtitleTrack1"),
      deleteR2KeyDeleteTaskStatement("root/subtitleTrack2"),
      deleteR2KeyDeleteTaskStatement("root/subtitleTrack3"),
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
            insertVideoContainerStatement("container1", {
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
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/subtitleTrack2",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              LIST_R2_KEY_DELETE_TASKS_ROW,
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
            insertVideoContainerStatement("container1", {
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
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
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
            insertVideoContainerStatement("container1", {
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
          await listR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
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
            insertVideoContainerStatement("container1", {
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
