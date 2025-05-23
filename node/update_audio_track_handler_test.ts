import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { UpdateAudioTrackHandler } from "./update_audio_track_handler";
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
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "UpdateAudioTrackHandlerTest",
  cases: [
    {
      name: "Update",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
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
            }),
          ]);
          await transaction.commit();
        });
        let handler = new UpdateAudioTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "container1",
          r2TrackDirname: "audioTrack1",
          name: "newName1",
          isDefault: false,
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
                videoContainerData: {
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
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
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
        let handler = new UpdateAudioTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "container1",
          r2TrackDirname: "audioTrack1",
          name: "newName2",
          isDefault: true,
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
                videoContainerData: {
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
                          name: "newName2",
                          isDefault: true,
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
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
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
            }),
          ]);
          await transaction.commit();
        });
        let handler = new UpdateAudioTrackHandler(SPANNER_DATABASE);

        // Execute
        let error = await assertReject(
          handler.handle("prefix", {
            containerId: "container1",
            r2TrackDirname: "audioTrack2",
            name: "newName2",
            isDefault: false,
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
