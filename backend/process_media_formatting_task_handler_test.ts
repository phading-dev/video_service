import { GCS_VIDEO_LOCAL_DIR, R2_VIDEO_LOCAL_DIR } from "../common/env_vars";
import { AUDIO_TRACKS_LIMIT } from "../common/params";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { AudioTrack, VIDEO_CONTAINER_DATA } from "../db/schema";
import {
  checkR2Key,
  deleteGcsFileCleanupTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyCleanupTaskStatement,
  deleteR2KeyStatement,
  deleteVideoContainerStatement,
  getGcsFileCleanupTasks,
  getMediaFormattingTasks,
  getR2KeyCleanupTasks,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import {
  eqGetGcsFileCleanupTasksRow,
  eqGetMediaFormattingTasksRow,
  eqGetR2KeyCleanupTasksRow,
} from "../db/test_matcher";
import { FailureReason } from "../failure_reason";
import { Source } from "../source";
import { ProcessMediaFormattingTaskHandler } from "./process_media_formatting_task_handler";
import { BlockingLoopMock } from "@selfage/blocking_loop/blocking_loop_mock";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { spawnSync } from "child_process";

async function insertOneVideoInFormatting(
  gcsVideoFilename: string,
): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement("container1", {
        source: Source.SHOW,
        r2Dirname: "container1",
        version: 0,
        processing: {
          media: {
            formatting: {
              gcsFilename: gcsVideoFilename,
            },
          },
        },
        videoTracks: [],
        audioTracks: [],
      }),
      insertMediaFormattingTaskStatement("container1", gcsVideoFilename, 0, 0),
    ]);
    await transaction.commit();
  });
}

let ALL_TEST_GCS_FILE = [
  "h265_opus_codec.mp4",
  "one_video_one_audio.mp4",
  "two_audios.mp4",
  "two_videos_two_audios.mp4",
  "video_only.mp4",
];

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteR2KeyStatement("container1/uuid0"),
      deleteR2KeyStatement("container1/uuid1"),
      deleteR2KeyStatement("container1/uuid2"),
      deleteR2KeyCleanupTaskStatement("container1/uuid0"),
      deleteR2KeyCleanupTaskStatement("container1/uuid1"),
      deleteR2KeyCleanupTaskStatement("container1/uuid2"),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsFileCleanupTaskStatement(gcsFilename),
      ),
    ]);
    await transaction.commit();
  });
  spawnSync("rm", ["-rf", `${R2_VIDEO_LOCAL_DIR}/container1`], {
    stdio: "inherit",
  });
  ALL_TEST_GCS_FILE.forEach((gcsFilename) =>
    spawnSync("rm", ["-f", `${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`], {
      stdio: "inherit",
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessMediaFormattingTaskHandlerTest",
  cases: [
    {
      name: "FormatOneVideoTrackTwoAudioTracks",
      execute: async () => {
        // Prepare
        spawnSync(
          "cp",
          ["test_data/two_videos_two_audios.mp4", GCS_VIDEO_LOCAL_DIR],
          {
            stdio: "inherit",
          },
        );
        await insertOneVideoInFormatting("two_videos_two_audios.mp4");
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_videos_two_audios.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              videoTracks: [
                {
                  toAdd: {
                    r2TrackDirname: "uuid0",
                    durationSec: 240,
                    resolution: "640x360",
                    totalBytes: 19618786,
                  },
                },
              ],
              audioTracks: [
                {
                  toAdd: {
                    name: "1",
                    isDefault: true,
                    r2TrackDirname: "uuid1",
                    totalBytes: 3158359,
                  },
                },
                {
                  toAdd: {
                    name: "2",
                    isDefault: false,
                    r2TrackDirname: "uuid2",
                    totalBytes: 3158359,
                  },
                },
              ],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid0")).length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid1")).length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid2")).length,
          eq(1),
          "audio 2 dir r2 key exists",
        );
        assertThat(id, eq(3), "ids used");
        assertThat(
          (await getMediaFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetGcsFileCleanupTasksRow({
              gcsFileCleanupTaskFilename: "two_videos_two_audios.mp4",
              gcsFileCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "gcs file cleanup tasks",
        );
        assertThat(
          (await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of r2 key cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "AppendOneVideoTrackAndOneAudioTrackToOneVideoTrackAndOneRemovingAudioTrack",
      execute: async () => {
        // Prepare
        spawnSync(
          "cp",
          ["test_data/one_video_one_audio.mp4", GCS_VIDEO_LOCAL_DIR],
          {
            stdio: "inherit",
          },
        );
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                media: {
                  formatting: {
                    gcsFilename: "one_video_one_audio.mp4",
                  },
                },
              },
              videoTracks: [
                {
                  data: {
                    r2TrackDirname: "video0",
                    totalBytes: 1000,
                  },
                },
              ],
              audioTracks: [
                {
                  data: {
                    name: "1",
                    isDefault: true,
                    r2TrackDirname: "audio0",
                    totalBytes: 1000,
                  },
                  toRemove: true,
                },
              ],
            }),
            insertMediaFormattingTaskStatement(
              "container1",
              "one_video_one_audio.mp4",
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              videoTracks: [
                {
                  data: {
                    r2TrackDirname: "video0",
                    totalBytes: 1000,
                  },
                },
                {
                  toAdd: {
                    r2TrackDirname: "uuid0",
                    durationSec: 240,
                    resolution: "640x360",
                    totalBytes: 19618786,
                  },
                },
              ],
              audioTracks: [
                {
                  data: {
                    name: "1",
                    isDefault: true,
                    r2TrackDirname: "audio0",
                    totalBytes: 1000,
                  },
                  toRemove: true,
                },
                {
                  toAdd: {
                    name: "1",
                    isDefault: true,
                    r2TrackDirname: "uuid1",
                    totalBytes: 3158359,
                  },
                },
              ],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid0")).length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid1")).length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(id, eq(2), "ids used");
        assertThat(
          (await getMediaFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetGcsFileCleanupTasksRow({
              gcsFileCleanupTaskFilename: "one_video_one_audio.mp4",
              gcsFileCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "gcs file cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "VideoOnly",
      execute: async () => {
        // Prepare
        spawnSync("cp", ["test_data/video_only.mp4", GCS_VIDEO_LOCAL_DIR], {
          stdio: "inherit",
        });
        await insertOneVideoInFormatting("video_only.mp4");
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "video_only.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              videoTracks: [
                {
                  toAdd: {
                    r2TrackDirname: "uuid0",
                    durationSec: 240,
                    resolution: "640x360",
                    totalBytes: 19618786,
                  },
                },
              ],
              audioTracks: [],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid0")).length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(id, eq(1), "ids used");
        assertThat(
          (await getMediaFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetGcsFileCleanupTasksRow({
              gcsFileCleanupTaskFilename: "video_only.mp4",
              gcsFileCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "gcs file cleanup tasks",
        );
        assertThat(
          (await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of r2 key cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ContainerNotInMediaFormattingState",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                subtitle: {},
              },
            }),
          ]);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            gcsFilename: "one_video_one_audio.mp4",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "Container container1 is not in media formatting state",
            ),
          ),
          "",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "AudioOnlyButTooManuAudioTracks",
      execute: async () => {
        // Prepare
        spawnSync("cp", ["test_data/two_audios.mp4", GCS_VIDEO_LOCAL_DIR], {
          stdio: "inherit",
        });
        let originalAudioTracks = new Array<AudioTrack>();
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          for (let i = 0; i < AUDIO_TRACKS_LIMIT; i++) {
            switch (i % 3) {
              case 0:
                originalAudioTracks.push({
                  data: {
                    name: `${i}`,
                    isDefault: false,
                    r2TrackDirname: `audio${i}`,
                    totalBytes: 1000,
                  },
                });
              case 1:
                originalAudioTracks.push({
                  data: {
                    name: `${i}`,
                    isDefault: false,
                    r2TrackDirname: `audio${i}`,
                    totalBytes: 1000,
                  },
                  toChange: {
                    name: `${i}`,
                    isDefault: true,
                  },
                });
              case 2:
                originalAudioTracks.push({
                  toAdd: {
                    name: `${i}`,
                    isDefault: false,
                    r2TrackDirname: `audio${i}`,
                    totalBytes: 1000,
                  },
                });
            }
          }
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                media: {
                  formatting: {
                    gcsFilename: "two_audios.mp4",
                  },
                },
              },
              videoTracks: [],
              audioTracks: originalAudioTracks,
            }),
            insertMediaFormattingTaskStatement(
              "container1",
              "two_audios.mp4",
              0,
              0,
            ),
          ]);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_audios.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                lastFailures: [FailureReason.AUDIO_TOO_MANY_TRACKS],
              },
              videoTracks: [],
              audioTracks: originalAudioTracks,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          (await getMediaFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetGcsFileCleanupTasksRow({
              gcsFileCleanupTaskFilename: "two_audios.mp4",
              gcsFileCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "gcs file cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "BothVideoCodecAndAudioCodecInvalid",
      execute: async () => {
        // Prepare
        spawnSync(
          "cp",
          ["test_data/h265_opus_codec.mp4", GCS_VIDEO_LOCAL_DIR],
          {
            stdio: "inherit",
          },
        );
        await insertOneVideoInFormatting("h265_opus_codec.mp4");
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "h265_opus_codec.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                lastFailures: [
                  FailureReason.VIDEO_CODEC_REQUIRES_H264,
                  FailureReason.AUDIO_CODEC_REQUIRES_AAC,
                ],
              },
              videoTracks: [],
              audioTracks: [],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          (await getMediaFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetGcsFileCleanupTasksRow({
              gcsFileCleanupTaskFilename: "h265_opus_codec.mp4",
              gcsFileCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "gcs file cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FormattingInterruptedUnexpectedly",
      execute: async () => {
        // Prepare
        spawnSync(
          "cp",
          ["test_data/one_video_one_audio.mp4", GCS_VIDEO_LOCAL_DIR],
          {
            stdio: "inherit",
          },
        );
        await insertOneVideoInFormatting("one_video_one_audio.mp4");
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );
        handler.setInterfereFormat(() =>
          Promise.reject(new Error("fake error")),
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                media: {
                  formatting: {
                    gcsFilename: "one_video_one_audio.mp4",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          await getMediaFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetMediaFormattingTasksRow({
              mediaFormattingTaskContainerId: "container1",
              mediaFormattingTaskGcsFilename: "one_video_one_audio.mp4",
              mediaFormattingTaskExecutionTimestamp: 481000,
            }),
          ]),
          "formatting tasks to retry",
        );
        assertThat(
          await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid0",
              r2KeyCleanupTaskExecutionTimestamp: 481000,
            }),
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid1",
              r2KeyCleanupTaskExecutionTimestamp: 481000,
            }),
          ]),
          "R2 keys to cleanup",
        );
        assertThat(
          (await getGcsFileCleanupTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "gcs files to cleanup",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledFormatting_DelayingTasksFurther_ResumeButAnotherFileIsBeingFormatted",
      execute: async () => {
        // Prepare
        spawnSync(
          "cp",
          ["test_data/one_video_one_audio.mp4", GCS_VIDEO_LOCAL_DIR],
          {
            stdio: "inherit",
          },
        );
        await insertOneVideoInFormatting("one_video_one_audio.mp4");
        let blockingLoop = new BlockingLoopMock();
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          blockingLoop,
          () => now,
          () => `uuid${id++}`,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((resolve1) => {
          handler.setInterfereFormat(() => {
            resolve1();
            return new Promise<void>((resolve2) => (stallResolveFn = resolve2));
          });
        });

        // Execute
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });
        await firstEncounter;

        // Verify
        assertThat(
          await getMediaFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetMediaFormattingTasksRow({
              mediaFormattingTaskContainerId: "container1",
              mediaFormattingTaskGcsFilename: "one_video_one_audio.mp4",
              mediaFormattingTaskExecutionTimestamp: 481000,
            }),
          ]),
          "initial delayed formatting tasks",
        );
        assertThat(
          await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid0",
              r2KeyCleanupTaskExecutionTimestamp: 481000,
            }),
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid1",
              r2KeyCleanupTaskExecutionTimestamp: 481000,
            }),
          ]),
          "R2 keys to cleanup",
        );

        // Prepare
        now = 100000;

        // Execute
        await blockingLoop.execute();

        // Verify
        assertThat(
          await getMediaFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetMediaFormattingTasksRow({
              mediaFormattingTaskContainerId: "container1",
              mediaFormattingTaskGcsFilename: "one_video_one_audio.mp4",
              mediaFormattingTaskExecutionTimestamp: 580000,
            }),
          ]),
          "further delayed formatting tasks",
        );
        assertThat(
          await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid0",
              r2KeyCleanupTaskExecutionTimestamp: 580000,
            }),
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid1",
              r2KeyCleanupTaskExecutionTimestamp: 580000,
            }),
          ]),
          "further delayed R2 keys to cleanup",
        );

        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            updateVideoContainerStatement(
              {
                source: Source.SHOW,
                r2Dirname: "container1",
                version: 0,
                processing: {
                  media: {
                    formatting: {
                      gcsFilename: "two_audios.mp4",
                    },
                  },
                },
                videoTracks: [],
                audioTracks: [],
              },
              "container1",
            ),
            deleteMediaFormattingTaskStatement(
              "container1",
              "one_video_one_audio.mp4",
            ),
            insertMediaFormattingTaskStatement(
              "container1",
              "two_audios.mp4",
              now,
              now,
            ),
          ]);
          await transaction.commit();
        });

        // Execute
        stallResolveFn();
        await new Promise<void>((resolve) => handler.setDoneCallback(resolve));

        // Verify
        assertThat(
          (await getVideoContainer(SPANNER_DATABASE, "container1"))[0]
            .videoContainerData,
          eqMessage(
            {
              source: Source.SHOW,
              r2Dirname: "container1",
              version: 0,
              processing: {
                media: {
                  formatting: {
                    gcsFilename: "two_audios.mp4",
                  },
                },
              },
              videoTracks: [],
              audioTracks: [],
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid0",
              r2KeyCleanupTaskExecutionTimestamp: 580000,
            }),
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/uuid1",
              r2KeyCleanupTaskExecutionTimestamp: 580000,
            }),
          ]),
          "remained R2 keys to cleanup",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
