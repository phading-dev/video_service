import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_VIDEO_CONTAINER_ROW,
  GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  getVideoContainerWritingToFileTask,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  insertVideoContainerWritingToFileTaskStatement,
  listPendingVideoContainerSyncingTasks,
  listPendingVideoContainerWritingToFileTasks,
} from "../db/sql";
import { CommitVideoContainerStagingDataHandler } from "./commit_video_container_staging_data_handler";
import {
  MAX_NUM_OF_AUDIO_TRACKS,
  MAX_NUM_OF_SUBTITLE_TRACKS,
} from "@phading/constants/video";
import {
  COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
  ValidationError,
} from "@phading/video_service_interface/node/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER, TestCase } from "@selfage/test_runner";

async function cleaupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteVideoContainerSyncingTaskStatement({
        videoContainerSyncingTaskContainerIdEq: "container1",
        videoContainerSyncingTaskVersionEq: 1,
      }),
      deleteVideoContainerSyncingTaskStatement({
        videoContainerSyncingTaskContainerIdEq: "container1",
        videoContainerSyncingTaskVersionEq: 2,
      }),
      deleteVideoContainerWritingToFileTaskStatement({
        videoContainerWritingToFileTaskContainerIdEq: "container1",
        videoContainerWritingToFileTaskVersionEq: 1,
      }),
      deleteVideoContainerWritingToFileTaskStatement({
        videoContainerWritingToFileTaskContainerIdEq: "container1",
        videoContainerWritingToFileTaskVersionEq: 2,
      }),
    ]);
    await transaction.commit();
  });
}

class CommitErrorTest implements TestCase {
  public constructor(
    public name: string,
    private videoContainerData: VideoContainer,
    private expectedError: ValidationError,
  ) {}
  public async execute() {
    // Prepare
    await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        insertVideoContainerStatement({
          containerId: "container1",
          data: this.videoContainerData,
        }),
      ]);
      await transaction.commit();
    });
    let handler = new CommitVideoContainerStagingDataHandler(
      SPANNER_DATABASE,
      () => 1000,
    );

    // Execute
    let response = await handler.handle("", {
      containerId: "container1",
    });

    // Verify
    assertThat(
      response,
      eqMessage(
        {
          error: this.expectedError,
          success: false,
        },
        COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
      ),
      "response",
    );
    assertThat(
      await getVideoContainer(SPANNER_DATABASE, {
        videoContainerContainerIdEq: "container1",
      }),
      isArray([
        eqMessage(
          {
            videoContainerContainerId: "container1",
            videoContainerData: this.videoContainerData,
          },
          GET_VIDEO_CONTAINER_ROW,
        ),
      ]),
      "video container",
    );
    assertThat(
      await listPendingVideoContainerWritingToFileTasks(SPANNER_DATABASE, {
        videoContainerWritingToFileTaskExecutionTimeMsLe: 10000000,
      }),
      isArray([]),
      "writing to file tasks",
    );
  }
  public async tearDown() {
    await cleaupAll();
  }
}

TEST_RUNNER.run({
  name: "CommitVideoContainerStagingDataHandlerTest",
  cases: [
    {
      name: "InitialCommit",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                masterPlaylist: {
                  synced: {
                    version: 0,
                    r2Filename: "0",
                  },
                },
                videoTracks: [
                  {
                    r2TrackDirname: "video1",
                    staging: {
                      toAdd: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
                audioTracks: [
                  {
                    r2TrackDirname: "audio1",
                    staging: {
                      toAdd: {
                        name: "Eng",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audio2",
                    staging: {
                      toAdd: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
                subtitleTracks: [
                  {
                    r2TrackDirname: "subtitle1",
                    staging: {
                      toAdd: {
                        name: "Eng",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitle2",
                    staging: {
                      toAdd: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CommitVideoContainerStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let response = await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              success: true,
            },
            COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
          ),
          "response",
        );
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerData: {
                  masterPlaylist: {
                    writingToFile: {
                      version: 1,
                      r2FilenamesToDelete: ["0"],
                      r2DirnamesToDelete: [],
                    },
                  },
                  videoTracks: [
                    {
                      r2TrackDirname: "video1",
                      committed: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 12345,
                      },
                    },
                  ],
                  audioTracks: [
                    {
                      r2TrackDirname: "audio1",
                      committed: {
                        name: "Eng",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "audio2",
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  ],
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitle1",
                      committed: {
                        name: "Eng",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "subtitle2",
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
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
          await getVideoContainerWritingToFileTask(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 1,
                videoContainerWritingToFileTaskRetryCount: 0,
                videoContainerWritingToFileTaskExecutionTimeMs: 1000,
                videoContainerWritingToFileTaskCreatedTimeMs: 1000,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW,
            ),
          ]),
          "writing to file tasks",
        );
      },
      tearDown: async () => {
        await cleaupAll();
      },
    },
    {
      name: "CommitMultipleUpdatesAndOverrideWritingToFile",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                masterPlaylist: {
                  writingToFile: {
                    version: 1,
                    r2FilenamesToDelete: ["master0"],
                    r2DirnamesToDelete: ["dir0"],
                  },
                },
                videoTracks: [
                  {
                    r2TrackDirname: "video1",
                    committed: {
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 12345,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "video2",
                    staging: {
                      toAdd: {
                        durationSec: 120,
                        resolution: "1280x720",
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
                audioTracks: [
                  {
                    r2TrackDirname: "audio1",
                    committed: {
                      name: "Eng",
                      isDefault: true,
                      totalBytes: 12345,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "audio2",
                    committed: {
                      name: "Jpn",
                      isDefault: false,
                      totalBytes: 12345,
                    },
                  },
                  {
                    r2TrackDirname: "audio3",
                    staging: {
                      toAdd: {
                        name: "Fra",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audio4",
                    committed: {
                      name: "a",
                      isDefault: false,
                      totalBytes: 12345,
                    },
                    staging: {
                      toAdd: {
                        name: "Spa",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
                subtitleTracks: [
                  {
                    r2TrackDirname: "subtitle1",
                    committed: {
                      name: "Eng",
                      isDefault: true,
                      totalBytes: 12345,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "subtitle2",
                    committed: {
                      name: "Jpn",
                      isDefault: false,
                      totalBytes: 12345,
                    },
                  },
                  {
                    r2TrackDirname: "subtitle3",
                    staging: {
                      toAdd: {
                        name: "Fra",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitle4",
                    committed: {
                      name: "a",
                      isDefault: false,
                      totalBytes: 12345,
                    },
                    staging: {
                      toAdd: {
                        name: "Spa",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  },
                ],
              },
            }),
            insertVideoContainerWritingToFileTaskStatement({
              containerId: "container1",
              version: 1,
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CommitVideoContainerStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let response = await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              success: true,
            },
            COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
          ),
          "response",
        );
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerData: {
                  masterPlaylist: {
                    writingToFile: {
                      version: 2,
                      r2FilenamesToDelete: ["master0"],
                      r2DirnamesToDelete: [
                        "dir0",
                        "video1",
                        "audio1",
                        "subtitle1",
                      ],
                    },
                  },
                  videoTracks: [
                    {
                      r2TrackDirname: "video2",
                      committed: {
                        durationSec: 120,
                        resolution: "1280x720",
                        totalBytes: 12345,
                      },
                    },
                  ],
                  audioTracks: [
                    {
                      r2TrackDirname: "audio2",
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "audio3",
                      committed: {
                        name: "Fra",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "audio4",
                      committed: {
                        name: "Spa",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                  ],
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitle2",
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "subtitle3",
                      committed: {
                        name: "Fra",
                        isDefault: true,
                        totalBytes: 12345,
                      },
                    },
                    {
                      r2TrackDirname: "subtitle4",
                      committed: {
                        name: "Spa",
                        isDefault: false,
                        totalBytes: 12345,
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
          await getVideoContainerWritingToFileTask(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 2,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 2,
                videoContainerWritingToFileTaskRetryCount: 0,
                videoContainerWritingToFileTaskExecutionTimeMs: 1000,
                videoContainerWritingToFileTaskCreatedTimeMs: 1000,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW,
            ),
          ]),
          "writing to file tasks",
        );
      },
      tearDown: async () => {
        await cleaupAll();
      },
    },
    {
      name: "CommitOverrideSyncing",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              data: {
                masterPlaylist: {
                  syncing: {
                    version: 1,
                    r2Filename: "master1",
                    r2FilenamesToDelete: ["master0"],
                    r2DirnamesToDelete: ["dir1"],
                  },
                },
                videoTracks: [
                  {
                    r2TrackDirname: "video1",
                    committed: {
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 12345,
                    },
                  },
                ],
                audioTracks: [],
                subtitleTracks: [],
              },
            }),
            insertVideoContainerSyncingTaskStatement({
              containerId: "container1",
              version: 1,
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CommitVideoContainerStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let response = await handler.handle("", {
          containerId: "container1",
        });

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              success: true,
            },
            COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
          ),
          "response",
        );
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerData: {
                  masterPlaylist: {
                    writingToFile: {
                      version: 2,
                      r2FilenamesToDelete: ["master0", "master1"],
                      r2DirnamesToDelete: ["dir1"],
                    },
                  },
                  videoTracks: [
                    {
                      r2TrackDirname: "video1",
                      committed: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 12345,
                      },
                    },
                  ],
                  audioTracks: [],
                  subtitleTracks: [],
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerWritingToFileTask(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 2,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 2,
                videoContainerWritingToFileTaskRetryCount: 0,
                videoContainerWritingToFileTaskExecutionTimeMs: 1000,
                videoContainerWritingToFileTaskCreatedTimeMs: 1000,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW,
            ),
          ]),
          "writing to file tasks",
        );
        assertThat(
          await listPendingVideoContainerSyncingTasks(SPANNER_DATABASE, {
            videoContainerSyncingTaskExecutionTimeMsLe: 10000000,
          }),
          isArray([]),
          "syncing tasks",
        );
      },
      tearDown: async () => {
        await cleaupAll();
      },
    },
    new CommitErrorTest(
      "NoVideoTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
            staging: {
              toDelete: true,
            },
          },
        ],
        audioTracks: [],
        subtitleTracks: [],
      },
      ValidationError.NO_VIDEO_TRACK,
    ),
    new CommitErrorTest(
      "MoreThanOneVideoTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
          {
            r2TrackDirname: "video2",
            staging: {
              toAdd: {
                durationSec: 120,
                resolution: "1280x720",
                totalBytes: 12345,
              },
            },
          },
        ],
        audioTracks: [],
        subtitleTracks: [],
      },
      ValidationError.MORE_THAN_ONE_VIDEO_TRACKS,
    ),
    new CommitErrorTest(
      "NoDefaultAudioTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio1",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
            staging: {
              toDelete: true,
            },
          },
          {
            r2TrackDirname: "audio2",
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: false,
                totalBytes: 12345,
              },
            },
          },
          {
            r2TrackDirname: "audio3",
            committed: {
              name: "Fra",
              isDefault: true,
              totalBytes: 12345,
            },
            staging: {
              toAdd: {
                name: "Fra",
                isDefault: false,
                totalBytes: 12345,
              },
            },
          },
        ],
        subtitleTracks: [],
      },
      ValidationError.NO_DEFAULT_AUDIO_TRACK,
    ),
    new CommitErrorTest(
      "MoreThanOneDefaultAudioTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio1",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
          },
          {
            r2TrackDirname: "audio2",
            committed: {
              name: "Jpn",
              isDefault: false,
              totalBytes: 12345,
            },
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: true,
                totalBytes: 12345,
              },
            },
          },
        ],
        subtitleTracks: [],
      },
      ValidationError.MORE_THAN_ONE_DEFAULT_AUDIO_TRACKS,
    ),
    new CommitErrorTest(
      "TooManyAudioTracks",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio0",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
          },
          ...Array.from({ length: MAX_NUM_OF_AUDIO_TRACKS }, (_, i) => ({
            r2TrackDirname: `audio${i + 1}`,
            committed: {
              name: `audio${i + 1}`,
              isDefault: false,
              totalBytes: 12345,
            },
          })),
        ],
        subtitleTracks: [],
      },
      ValidationError.TOO_MANY_AUDIO_TRACKS,
    ),
    new CommitErrorTest(
      "NoDefaultSubtitleTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [],
        subtitleTracks: [
          {
            r2TrackDirname: "subtitle1",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
            staging: {
              toDelete: true,
            },
          },
          {
            r2TrackDirname: "subtitle2",
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: false,
                totalBytes: 12345,
              },
            },
          },
          {
            r2TrackDirname: "subtitle3",
            committed: {
              name: "Fra",
              isDefault: true,
              totalBytes: 12345,
            },
            staging: {
              toAdd: {
                name: "Fra",
                isDefault: false,
                totalBytes: 12345,
              },
            },
          },
        ],
      },
      ValidationError.NO_DEFAULT_SUBTITLE_TRACK,
    ),
    new CommitErrorTest(
      "MoreThanOneDefaultSubtitleTrack",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [],
        subtitleTracks: [
          {
            r2TrackDirname: "subtitle1",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
          },
          {
            r2TrackDirname: "subtitle2",
            committed: {
              name: "Jpn",
              isDefault: false,
              totalBytes: 12345,
            },
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: true,
                totalBytes: 12345,
              },
            },
          },
        ],
      },
      ValidationError.MORE_THAN_ONE_DEFAULT_SUBTITLE_TRACKS,
    ),
    new CommitErrorTest(
      "TooManySubtitleTracks",
      {
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            committed: {
              durationSec: 60,
              resolution: "1920x1080",
              totalBytes: 12345,
            },
          },
        ],
        audioTracks: [],
        subtitleTracks: [
          {
            r2TrackDirname: "subtitle0",
            committed: {
              name: "Eng",
              isDefault: true,
              totalBytes: 12345,
            },
          },
          ...Array.from({ length: MAX_NUM_OF_SUBTITLE_TRACKS }, (_, i) => ({
            r2TrackDirname: `subtitle${i + 1}`,
            committed: {
              name: `subtitle${i + 1}`,
              isDefault: false,
              totalBytes: 12345,
            },
          })),
        ],
      },
      ValidationError.TOO_MANY_SUBTITLE_TRACKS,
    ),
  ],
});
