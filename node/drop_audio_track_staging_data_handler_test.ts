import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_R2_KEY_DELETE_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteR2KeyDeleteTaskStatement,
  deleteVideoContainerStatement,
  getR2KeyDeleteTasks,
  getVideoContainer,
  insertVideoContainerStatement,
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
      deleteVideoContainerStatement("container1"),
      deleteR2KeyDeleteTaskStatement("root/audioTrack1"),
      deleteR2KeyDeleteTaskStatement("root/audioTrack2"),
      deleteR2KeyDeleteTaskStatement("root/audioTrack3"),
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
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
                  staging: {
                    toAdd: {
                      name: "name1",
                      isDefault: true,
                      totalBytes: 100,
                    },
                  },
                },
                {
                  r2TrackDirname: "audioTrack2",
                  staging: {
                    toAdd: {
                      name: "name2",
                      isDefault: false,
                      totalBytes: 100,
                    },
                  },
                },
                {
                  r2TrackDirname: "audioTrack3",
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
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  r2RootDirname: "root",
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      staging: {
                        toAdd: {
                          name: "name1",
                          isDefault: true,
                          totalBytes: 100,
                        },
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack3",
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
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/audioTrack2",
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
      name: "DropToUpdate",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
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
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  r2RootDirname: "root",
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
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
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
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
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "audioTrack2",
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
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  r2RootDirname: "root",
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      committed: {
                        name: "name1",
                        isDefault: true,
                        totalBytes: 100,
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack2",
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
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 10000000),
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
            insertVideoContainerStatement("container1", {
              r2RootDirname: "root",
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "audioTrack3",
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
