import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { UpdateSubtitleTrackHandler } from "./update_subtitle_track_handler";
import { newNotFoundError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("containerId"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "UpdateSubtitleTrackHandlerTest",
  cases: [
    {
      name: "Update",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("containerId", {
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
            }),
          ]);
          await transaction.commit();
        });
        let handler = new UpdateSubtitleTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "containerId",
          r2TrackDirname: "subtitleTrack1",
          name: "newName1",
          isDefault: false,
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "containerId"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
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
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "UpdateStaging",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("containerId", {
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
        let handler = new UpdateSubtitleTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "containerId",
          r2TrackDirname: "subtitleTrack1",
          name: "newName2",
          isDefault: true,
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "containerId"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
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
                          name: "newName2",
                          isDefault: true,
                          totalBytes: 100,
                        },
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
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotFound",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("containerId", {
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
            }),
          ]);
          await transaction.commit();
        });
        let handler = new UpdateSubtitleTrackHandler(SPANNER_DATABASE);

        // Execute
        let error = await assertReject(
          handler.handle("prefix", {
            containerId: "containerId",
            r2TrackDirname: "subtitleTrack2",
            name: "newName2",
            isDefault: false,
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
