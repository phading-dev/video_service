import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_END_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_ROW,
  deleteR2KeyDeletingTaskStatement,
  deleteStorageEndRecordingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getR2KeyDeletingTask,
  getStorageEndRecordingTask,
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
import { COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE } from "@phading/video_service_interface/node/interface";
import { ValidationError } from "@phading/video_service_interface/node/validation_error";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER, TestCase } from "@selfage/test_runner";

async function cleaupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/video3",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/audio4",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/subtitle4",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/video3",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/audio4",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/subtitle4",
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
          accountId: "account1",
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
      videoContainer: {
        videos: this.videoContainerData.videoTracks,
        audios: this.videoContainerData.audioTracks,
        subtitles: this.videoContainerData.subtitleTracks,
      },
    });

    // Verify
    assertThat(
      response,
      eqMessage(
        {
          error: this.expectedError,
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
            videoContainerAccountId: "account1",
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
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                masterPlaylist: {
                  synced: {
                    version: 0,
                    r2Filename: "0",
                  },
                },
                videoTracks: [
                  {
                    r2TrackDirname: "video1",
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 12345,
                    staging: {
                      toAdd: true,
                    },
                  },
                ],
                audioTracks: [
                  {
                    r2TrackDirname: "audio1",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "1",
                        isDefault: true,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audio2",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "2",
                        isDefault: false,
                      },
                    },
                  },
                ],
                subtitleTracks: [
                  {
                    r2TrackDirname: "subtitle1",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "1",
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitle2",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "2",
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
          videoContainer: {
            videos: [
              {
                r2TrackDirname: "video1",
                staging: {
                  toAdd: true,
                },
              },
            ],
            audios: [
              {
                r2TrackDirname: "audio1",
                staging: {
                  toAdd: {
                    name: "Eng",
                    isDefault: true,
                  },
                },
              },
              {
                r2TrackDirname: "audio2",
                staging: {
                  toAdd: {
                    name: "Jpn",
                    isDefault: false,
                  },
                },
              },
            ],
            subtitles: [
              {
                r2TrackDirname: "subtitle1",
                staging: {
                  toAdd: {
                    name: "Eng",
                  },
                },
              },
              {
                r2TrackDirname: "subtitle2",
                staging: {
                  toAdd: {
                    name: "Jpn",
                  },
                },
              },
            ],
          },
        });

        // Verify
        assertThat(
          response,
          eqMessage({}, COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE),
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
                videoContainerAccountId: "account1",
                videoContainerData: {
                  r2RootDirname: "root",
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
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 12345,
                      committed: true,
                    },
                  ],
                  audioTracks: [
                    {
                      r2TrackDirname: "audio1",
                      totalBytes: 12345,
                      committed: {
                        name: "Eng",
                        isDefault: true,
                      },
                    },
                    {
                      r2TrackDirname: "audio2",
                      totalBytes: 12345,
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                      },
                    },
                  ],
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitle1",
                      totalBytes: 12345,
                      committed: {
                        name: "Eng",
                      },
                    },
                    {
                      r2TrackDirname: "subtitle2",
                      totalBytes: 12345,
                      committed: {
                        name: "Jpn",
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
              accountId: "account1",
              data: {
                r2RootDirname: "root",
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
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 12345,
                    committed: true,
                  },
                  {
                    r2TrackDirname: "video2",
                    durationSec: 120,
                    resolution: "1280x720",
                    totalBytes: 12345,
                    staging: {
                      toAdd: true,
                    },
                  },
                  {
                    r2TrackDirname: "video3",
                    durationSec: 180,
                    resolution: "640x360",
                    totalBytes: 12345,
                    staging: {
                      toAdd: true,
                    },
                  },
                ],
                audioTracks: [
                  {
                    r2TrackDirname: "audio1",
                    totalBytes: 12345,
                    committed: {
                      name: "Eng",
                      isDefault: true,
                    },
                  },
                  {
                    r2TrackDirname: "audio2",
                    totalBytes: 12345,
                    committed: {
                      name: "Jpn",
                      isDefault: false,
                    },
                  },
                  {
                    r2TrackDirname: "audio3",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "1",
                        isDefault: true,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audio4",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "2",
                        isDefault: false,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audio5",
                    totalBytes: 12345,
                    committed: {
                      name: "a",
                      isDefault: false,
                    },
                    staging: {
                      toAdd: {
                        name: "Spa",
                        isDefault: false,
                      },
                    },
                  },
                ],
                subtitleTracks: [
                  {
                    r2TrackDirname: "subtitle1",
                    totalBytes: 12345,
                    committed: {
                      name: "Eng",
                    },
                  },
                  {
                    r2TrackDirname: "subtitle2",
                    totalBytes: 12345,
                    committed: {
                      name: "Jpn",
                    },
                  },
                  {
                    r2TrackDirname: "subtitle3",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "1",
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitle4",
                    totalBytes: 12345,
                    staging: {
                      toAdd: {
                        name: "2",
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitle5",
                    totalBytes: 12345,
                    committed: {
                      name: "a",
                    },
                    staging: {
                      toAdd: {
                        name: "Spa",
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
          videoContainer: {
            videos: [
              {
                r2TrackDirname: "video1",
                staging: {
                  toDelete: true,
                },
              },
              {
                r2TrackDirname: "video2",
                staging: {
                  toAdd: true,
                },
              },
              {
                r2TrackDirname: "video3",
              },
            ],
            audios: [
              {
                r2TrackDirname: "audio1",
                staging: {
                  toDelete: true,
                },
              },
              {
                r2TrackDirname: "audio2",
              },
              {
                r2TrackDirname: "audio3",
                staging: {
                  toAdd: {
                    name: "Fra",
                    isDefault: true,
                  },
                },
              },
              {
                r2TrackDirname: "audio4",
              },
              {
                r2TrackDirname: "audio5",
                staging: {
                  toAdd: {
                    name: "Spa",
                    isDefault: false,
                  },
                },
              },
            ],
            subtitles: [
              {
                r2TrackDirname: "subtitle1",
                staging: {
                  toDelete: true,
                },
              },
              {
                r2TrackDirname: "subtitle2",
              },
              {
                r2TrackDirname: "subtitle3",
                staging: {
                  toAdd: {
                    name: "Fra",
                  },
                },
              },
              {
                r2TrackDirname: "subtitle4",
              },
              {
                r2TrackDirname: "subtitle5",
                staging: {
                  toAdd: {
                    name: "Spa",
                  },
                },
              },
            ],
          },
        });

        // Verify
        assertThat(
          response,
          eqMessage({}, COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE),
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
                videoContainerAccountId: "account1",
                videoContainerData: {
                  r2RootDirname: "root",
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
                      durationSec: 120,
                      resolution: "1280x720",
                      totalBytes: 12345,
                      committed: true,
                    },
                  ],
                  audioTracks: [
                    {
                      r2TrackDirname: "audio2",
                      totalBytes: 12345,
                      committed: {
                        name: "Jpn",
                        isDefault: false,
                      },
                    },
                    {
                      r2TrackDirname: "audio3",
                      totalBytes: 12345,
                      committed: {
                        name: "Fra",
                        isDefault: true,
                      },
                    },
                    {
                      r2TrackDirname: "audio5",
                      totalBytes: 12345,
                      committed: {
                        name: "Spa",
                        isDefault: false,
                      },
                    },
                  ],
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitle2",
                      totalBytes: 12345,
                      committed: {
                        name: "Jpn",
                      },
                    },
                    {
                      r2TrackDirname: "subtitle3",
                      totalBytes: 12345,
                      committed: {
                        name: "Fra",
                      },
                    },
                    {
                      r2TrackDirname: "subtitle5",
                      totalBytes: 12345,
                      committed: {
                        name: "Spa",
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
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/video3",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/video3",
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
          "storage end recording task for video3",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/audio4",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/audio4",
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
          "storage end recording task for audio4",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/subtitle4",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/subtitle4",
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
          "storage end recording task for subtitle4",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/video3",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/video3",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for video3",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/audio4",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/audio4",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for audio4",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/subtitle4",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/subtitle4",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for subtitle4",
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
              accountId: "account1",
              data: {
                r2RootDirname: "root",
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
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 12345,
                    committed: true,
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
          videoContainer: {
            videos: [
              {
                r2TrackDirname: "video1",
              },
            ],
            audios: [],
            subtitles: [],
          },
        });

        // Verify
        assertThat(
          response,
          eqMessage({}, COMMIT_VIDEO_CONTAINER_STAGING_DATA_RESPONSE),
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
                videoContainerAccountId: "account1",
                videoContainerData: {
                  r2RootDirname: "root",
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
                      durationSec: 60,
                      resolution: "1920x1080",
                      totalBytes: 12345,
                      committed: true,
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
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
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
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
          },
          {
            r2TrackDirname: "video2",
            durationSec: 120,
            resolution: "1280x720",
            totalBytes: 12345,
            staging: {
              toAdd: true,
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
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio1",
            totalBytes: 12345,
            committed: {
              name: "Eng",
              isDefault: true,
            },
            staging: {
              toDelete: true,
            },
          },
          {
            r2TrackDirname: "audio2",
            totalBytes: 12345,
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: false,
              },
            },
          },
          {
            r2TrackDirname: "audio3",
            totalBytes: 12345,
            committed: {
              name: "Fra",
              isDefault: true,
            },
            staging: {
              toAdd: {
                name: "Fra",
                isDefault: false,
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
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio1",
            totalBytes: 12345,
            committed: {
              name: "Eng",
              isDefault: true,
            },
          },
          {
            r2TrackDirname: "audio2",
            totalBytes: 12345,
            committed: {
              name: "Jpn",
              isDefault: false,
            },
            staging: {
              toAdd: {
                name: "Jpn",
                isDefault: true,
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
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audio0",
            totalBytes: 12345,
            committed: {
              name: "Eng",
              isDefault: true,
            },
          },
          ...Array.from({ length: MAX_NUM_OF_AUDIO_TRACKS }, (_, i) => ({
            r2TrackDirname: `audio${i + 1}`,
            totalBytes: 12345,
            committed: {
              name: `audio${i + 1}`,
              isDefault: false,
            },
          })),
        ],
        subtitleTracks: [],
      },
      ValidationError.TOO_MANY_AUDIO_TRACKS,
    ),
    new CommitErrorTest(
      "TooManySubtitleTracks",
      {
        r2RootDirname: "root",
        masterPlaylist: {
          synced: {
            version: 0,
            r2Filename: "0",
          },
        },
        videoTracks: [
          {
            r2TrackDirname: "video1",
            durationSec: 60,
            resolution: "1920x1080",
            totalBytes: 12345,
            committed: true,
          },
        ],
        audioTracks: [],
        subtitleTracks: [
          {
            r2TrackDirname: "subtitle0",
            totalBytes: 12345,
            committed: {
              name: "Eng",
            },
          },
          ...Array.from({ length: MAX_NUM_OF_SUBTITLE_TRACKS }, (_, i) => ({
            r2TrackDirname: `subtitle${i + 1}`,
            totalBytes: 12345,
            committed: {
              name: `subtitle${i + 1}`,
            },
          })),
        ],
      },
      ValidationError.TOO_MANY_SUBTITLE_TRACKS,
    ),
  ],
});
