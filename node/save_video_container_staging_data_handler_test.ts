import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
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
} from "../db/sql";
import { SaveVideoContainerStagingDataHandler } from "./save_video_container_staging_data_handler";
import { SAVE_VIDEO_CONTAINER_STAGING_DATA_RESPONSE } from "@phading/video_service_interface/node/interface";
import { ValidationError } from "@phading/video_service_interface/node/validation_error";
import { VideoContainerStagingData } from "@phading/video_service_interface/node/video_container_staging_data";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER, TestCase } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/videoTrack2",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/audioTrack2",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/subtitleTrack2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/videoTrack2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/audioTrack2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/subtitleTrack2",
      }),
    ]);
    await transaction.commit();
  });
}

class TrackMismatchTestCase implements TestCase {
  public constructor(
    public name: string,
    private videoContainer: VideoContainer,
    private videoContainerStagingData: VideoContainerStagingData,
  ) {}
  public async execute() {
    // Prepare
    await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
      await transaction.batchUpdate([
        insertVideoContainerStatement({
          containerId: "container1",
          data: this.videoContainer,
        }),
      ]);
      await transaction.commit();
    });
    let handler = new SaveVideoContainerStagingDataHandler(
      SPANNER_DATABASE,
      () => 1000,
    );

    // Execute
    let response = await handler.handle("", {
      containerId: "container1",
      videoContainer: this.videoContainerStagingData,
    });

    // Verify
    assertThat(
      response,
      eqMessage(
        {
          error: ValidationError.TRACK_MISMATCH,
        },
        SAVE_VIDEO_CONTAINER_STAGING_DATA_RESPONSE,
      ),
      "response",
    );
  }
  public async tearDown() {
    await cleanupAll();
  }
}

TEST_RUNNER.run({
  name: "SaveVideoContainerStagingDataHandlerTest",
  cases: [
    {
      name: "ManyUpdates",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement({
              containerId: "container1",
              accountId: "account1",
              data: {
                r2RootDirname: "root",
                videoTracks: [
                  {
                    r2TrackDirname: "videoTrack1",
                    totalBytes: 100,
                    committed: true,
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack2",
                    totalBytes: 200,
                    staging: {
                      toAdd: true,
                    },
                  },
                  {
                    r2TrackDirname: "videoTrack3",
                    totalBytes: 300,
                    committed: true,
                  },
                ],
                audioTracks: [
                  {
                    r2TrackDirname: "audioTrack1",
                    totalBytes: 1000,
                    committed: {
                      name: "name1",
                      isDefault: true,
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack2",
                    totalBytes: 2000,
                    staging: {
                      toAdd: {
                        name: "name2",
                        isDefault: false,
                      },
                    },
                  },
                  {
                    r2TrackDirname: "audioTrack3",
                    totalBytes: 3000,
                    committed: {
                      name: "name3",
                      isDefault: false,
                    },
                  },
                ],
                subtitleTracks: [
                  {
                    r2TrackDirname: "subtitleTrack1",
                    totalBytes: 10000,
                    committed: {
                      name: "subtitle1",
                    },
                    staging: {
                      toDelete: true,
                    },
                  },
                  {
                    r2TrackDirname: "subtitleTrack2",
                    totalBytes: 20000,
                    staging: {
                      toAdd: {
                        name: "subtitle2",
                      },
                    },
                  },
                  {
                    r2TrackDirname: "subtitleTrack3",
                    totalBytes: 30000,
                    committed: {
                      name: "subtitle3",
                    },
                  },
                ],
              },
            }),
          ]);
          await transaction.commit();
        });
        let handler = new SaveVideoContainerStagingDataHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        let response = await handler.handle("", {
          containerId: "container1",
          videoContainer: {
            videos: [
              {
                r2TrackDirname: "videoTrack1",
              },
              {
                r2TrackDirname: "videoTrack2",
              },
              {
                r2TrackDirname: "videoTrack3",
                staging: {
                  toDelete: true,
                },
              },
            ],
            audios: [
              {
                r2TrackDirname: "audioTrack1",
              },
              {
                r2TrackDirname: "audioTrack2",
              },
              {
                r2TrackDirname: "audioTrack3",
                staging: {
                  toAdd: {
                    name: "newName3",
                    isDefault: true,
                  },
                },
              },
            ],
            subtitles: [
              {
                r2TrackDirname: "subtitleTrack1",
              },
              {
                r2TrackDirname: "subtitleTrack2",
              },
              {
                r2TrackDirname: "subtitleTrack3",
                staging: {
                  toAdd: {
                    name: "newSubtitle3",
                  },
                },
              },
            ],
          },
        });

        // Verify
        assertThat(
          response,
          eqMessage({}, SAVE_VIDEO_CONTAINER_STAGING_DATA_RESPONSE),
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
                  videoTracks: [
                    {
                      r2TrackDirname: "videoTrack1",
                      totalBytes: 100,
                      committed: true,
                    },
                    {
                      r2TrackDirname: "videoTrack3",
                      totalBytes: 300,
                      committed: true,
                      staging: {
                        toDelete: true,
                      },
                    },
                  ],
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      totalBytes: 1000,
                      committed: {
                        name: "name1",
                        isDefault: true,
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack3",
                      totalBytes: 3000,
                      committed: {
                        name: "name3",
                        isDefault: false,
                      },
                      staging: {
                        toAdd: {
                          name: "newName3",
                          isDefault: true,
                        },
                      },
                    },
                  ],
                  subtitleTracks: [
                    {
                      r2TrackDirname: "subtitleTrack1",
                      totalBytes: 10000,
                      committed: {
                        name: "subtitle1",
                      },
                    },
                    {
                      r2TrackDirname: "subtitleTrack3",
                      totalBytes: 30000,
                      committed: {
                        name: "subtitle3",
                      },
                      staging: {
                        toAdd: {
                          name: "newSubtitle3",
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
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/videoTrack2",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/videoTrack2",
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
          "storage end recording task for videoTrack2",
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
          "storage end recording task for audioTrack2",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/subtitleTrack2",
          }),
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
          "storage end recording task for subtitleTrack2",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/videoTrack2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/videoTrack2",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for videoTrack2",
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
          "r2 key delete task for audioTrack2",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/subtitleTrack2",
          }),
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
          "r2 key delete task for subtitleTrack2",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    new TrackMismatchTestCase(
      "VideoTrackLengthMismatch",
      {
        videoTracks: [],
        audioTracks: [],
        subtitleTracks: [],
      },
      {
        videos: [
          {
            r2TrackDirname: "videoTrack1",
          },
        ],
        audios: [],
        subtitles: [],
      },
    ),
    new TrackMismatchTestCase(
      "AudioTrackLengthMismatch",
      {
        videoTracks: [],
        audioTracks: [
          {
            r2TrackDirname: "audioTrack1",
          },
        ],
        subtitleTracks: [],
      },
      {
        videos: [],
        audios: [
          {
            r2TrackDirname: "audioTrack1",
          },
          {
            r2TrackDirname: "audioTrack2",
          },
        ],
        subtitles: [],
      },
    ),
    new TrackMismatchTestCase(
      "SubtitleTrackLengthMismatch",
      {
        videoTracks: [],
        audioTracks: [],
        subtitleTracks: [],
      },
      {
        videos: [],
        audios: [],
        subtitles: [
          {
            r2TrackDirname: "subtitleTrack1",
          },
        ],
      },
    ),
    new TrackMismatchTestCase(
      "VideoTrackMismatch",
      {
        videoTracks: [
          {
            r2TrackDirname: "videoTrack1",
          },
          {
            r2TrackDirname: "videoTrack3",
          },
          {
            r2TrackDirname: "videoTrack4",
          },
        ],
        audioTracks: [],
        subtitleTracks: [],
      },
      {
        videos: [
          {
            r2TrackDirname: "videoTrack1",
          },
          {
            r2TrackDirname: "videoTrack2",
          },
          {
            r2TrackDirname: "videoTrack4",
          },
        ],
        audios: [],
        subtitles: [],
      },
    ),
    new TrackMismatchTestCase(
      "AudioTrackMismatch",
      {
        videoTracks: [
          {
            r2TrackDirname: "videoTrack1",
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audioTrack1",
          },
        ],
        subtitleTracks: [],
      },
      {
        videos: [
          {
            r2TrackDirname: "videoTrack1",
          },
        ],
        audios: [
          {
            r2TrackDirname: "audioTrack2",
          },
        ],
        subtitles: [],
      },
    ),
    new TrackMismatchTestCase(
      "SubtitleTrackMismatch",
      {
        videoTracks: [
          {
            r2TrackDirname: "videoTrack1",
          },
        ],
        audioTracks: [
          {
            r2TrackDirname: "audioTrack1",
          },
          {
            r2TrackDirname: "audioTrack2",
          },
        ],
        subtitleTracks: [
          {
            r2TrackDirname: "subtitleTrack1",
          },
        ],
      },
      {
        videos: [
          {
            r2TrackDirname: "videoTrack1",
          },
        ],
        audios: [
          {
            r2TrackDirname: "audioTrack1",
          },
          {
            r2TrackDirname: "audioTrack2",
          },
        ],
        subtitles: [
          {
            r2TrackDirname: "subtitleTrack2",
          },
        ],
      },
    ),
  ],
});
