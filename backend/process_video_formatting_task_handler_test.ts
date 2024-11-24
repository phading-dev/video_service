import { GCS_VIDEO_LOCAL_DIR, R2_VIDEO_LOCAL_DIR } from "../common/env_vars";
import { AUDIO_TRACKS_LIMIT } from "../common/params";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VIDEO_CONTAINER_DATA } from "../db/schema";
import {
  checkR2Key,
  deleteGcsFileCleanupTaskStatement,
  deleteR2KeyCleanupTaskStatement,
  deleteR2KeyStatement,
  deleteVideoContainerStatement,
  deleteVideoFormattingTaskStatement,
  getAllAudioTracks,
  getAllVideoTracks,
  getGcsFileCleanupTasks,
  getR2KeyCleanupTasks,
  getVideoContainer,
  getVideoContainerSyncingTasks,
  getVideoFormattingTasks,
  insertAudioTrackStatement,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  insertVideoFormattingTaskStatement,
  insertVideoTrackStatement,
  updateVideoTrackStatement,
} from "../db/sql";
import {
  eqGetAllAudioTracksRow,
  eqGetAllVideoTracksRow,
  eqGetGcsFileCleanupTasksRow,
  eqGetR2KeyCleanupTasksRow,
  eqGetVideoContainerSyncingTasksRow,
  eqGetVideoFormattingTasksRow,
} from "../db/test_matcher";
import { FailureReason } from "../failure_reason";
import { Source } from "../source";
import { ProcessVideoFormattingTaskHandler } from "./process_video_formatting_task_handler";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { BlockingLoopMock } from "@selfage/blocking_loop/blocking_loop_mock";
import { newNotFoundError } from "@selfage/http_error";
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
        totalBytes: 0,
        version: 0,
      }),
      insertVideoTrackStatement("container1", "video1", {
        formatting: {
          gcsFilename: gcsVideoFilename,
        },
      }),
      insertVideoFormattingTaskStatement("container1", "video1", 0, 0),
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

async function cleanupAll(gcsFilename: string): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteR2KeyStatement("container1/uuid0"),
      deleteR2KeyStatement("container1/uuid1"),
      deleteR2KeyStatement("container1/uuid2"),
      deleteR2KeyCleanupTaskStatement("container1/uuid0"),
      deleteR2KeyCleanupTaskStatement("container1/uuid1"),
      deleteR2KeyCleanupTaskStatement("container1/uuid2"),
      deleteR2KeyCleanupTaskStatement("container1/video0"),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsFileCleanupTaskStatement(gcsFilename),
      ),
    ]);
    await transaction.commit();
  });
  // spawnSync("rm", ["-rf", `${R2_VIDEO_LOCAL_DIR}/container1`], {
  //   stdio: "inherit",
  // });
  ALL_TEST_GCS_FILE.forEach((gcsFilename) =>
    spawnSync("rm", ["-f", `${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`], {
      stdio: "inherit",
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessVideoFormattingTaskHandlerTest",
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
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          videoId: "video1",
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
              totalBytes: 25935504,
              version: 1,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                done: {
                  r2TrackDirname: "uuid0",
                  durationSec: 240,
                  resolution: "640x360",
                  totalBytes: 19618786,
                },
              },
            }),
          ]),
          "video tracks",
        );
        assertThat(
          await getAllAudioTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllAudioTracksRow({
              audioTrackAudioId: "uuid1",
              audioTrackData: {
                name: "1",
                isDefault: true,
                done: {
                  r2TrackDirname: "uuid1",
                  totalBytes: 3158359,
                },
              },
            }),
            eqGetAllAudioTracksRow({
              audioTrackAudioId: "uuid2",
              audioTrackData: {
                name: "2",
                isDefault: false,
                done: {
                  r2TrackDirname: "uuid2",
                  totalBytes: 3158359,
                },
              },
            }),
          ]),
          "audio tracks",
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
          (await getVideoFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoContainerSyncingTasksRow({
              videoContainerSyncingTaskContainerId: "container1",
              videoContainerSyncingTaskVersion: 1,
              videoContainerSyncingTaskExecutionTimestamp: 1000,
            }),
          ]),
          "container syncing tasks",
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
        await cleanupAll("two_videos_two_audios.mp4");
      },
    },
    {
      name: "ReplaceExistingVideoTrackWhileAppendMoreToExistingAudioTracksWhileDeleteOldContainerSyncingTask",
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
              totalBytes: 1000,
              version: 0,
            }),
            insertVideoContainerSyncingTaskStatement("container1", 0, 10, 10),
            insertVideoTrackStatement("container1", "video0", {
              done: {
                r2TrackDirname: "video0",
                totalBytes: 1000,
              },
            }),
            insertVideoTrackStatement("container1", "video1", {
              formatting: {
                gcsFilename: "one_video_one_audio.mp4",
              },
            }),
            insertAudioTrackStatement("container1", "audio0", {}),
            insertVideoFormattingTaskStatement("container1", "video1", 0, 0),
          ]);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          videoId: "video1",
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
              totalBytes: 22777145,
              version: 1,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                done: {
                  r2TrackDirname: "uuid0",
                  durationSec: 240,
                  resolution: "640x360",
                  totalBytes: 19618786,
                },
              },
            }),
          ]),
          "video tracks",
        );
        assertThat(
          await getAllAudioTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllAudioTracksRow({
              audioTrackAudioId: "audio0",
              audioTrackData: {},
            }),
            eqGetAllAudioTracksRow({
              audioTrackAudioId: "uuid1",
              audioTrackData: {
                name: "2",
                isDefault: false,
                done: {
                  r2TrackDirname: "uuid1",
                  totalBytes: 3158359,
                },
              },
            }),
          ]),
          "audio tracks",
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
          (await getVideoFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoContainerSyncingTasksRow({
              videoContainerSyncingTaskContainerId: "container1",
              videoContainerSyncingTaskVersion: 1,
              videoContainerSyncingTaskExecutionTimestamp: 1000,
            }),
          ]),
          "container syncing tasks",
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
        assertThat(
          await getR2KeyCleanupTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetR2KeyCleanupTasksRow({
              r2KeyCleanupTaskKey: "container1/video0",
              r2KeyCleanupTaskExecutionTimestamp: 1000,
            }),
          ]),
          "r2 key cleanup tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll("one_video_one_audio.mp4");
      },
    },
    {
      name: "NoAudioTrack",
      execute: async () => {
        // Prepare
        spawnSync("cp", ["test_data/video_only.mp4", GCS_VIDEO_LOCAL_DIR], {
          stdio: "inherit",
        });
        await insertOneVideoInFormatting("video_only.mp4");
        let now = 1000;
        let id = 0;
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          videoId: "video1",
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
              totalBytes: 19618786,
              version: 1,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                done: {
                  r2TrackDirname: "uuid0",
                  durationSec: 240,
                  resolution: "640x360",
                  totalBytes: 19618786,
                },
              },
            }),
          ]),
          "video tracks",
        );
        assertThat(
          (await getAllAudioTracks(SPANNER_DATABASE, "container1")).length,
          eq(0),
          "# of audio tracks",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "container1/uuid0")).length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(id, eq(1), "ids used");
        assertThat(
          (await getVideoFormattingTasks(SPANNER_DATABASE, 1000000)).length,
          eq(0),
          "# of video formatting tasks",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoContainerSyncingTasksRow({
              videoContainerSyncingTaskContainerId: "container1",
              videoContainerSyncingTaskVersion: 1,
              videoContainerSyncingTaskExecutionTimestamp: 1000,
            }),
          ]),
          "container syncing tasks",
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
        await cleanupAll("video_only.mp4");
      },
    },
    {
      name: "VideoIsNotInFormattingState",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              source: Source.SHOW,
              r2Dirname: "container1",
              totalBytes: 0,
              version: 0,
            }),
            insertVideoTrackStatement("container1", "video1", {
              failure: {},
            }),
          ]);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            videoId: "video1",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newNotFoundError("video video1 is not in formatting state"),
          ),
          "",
        );
      },
      tearDown: async () => {
        await cleanupAll("video_only.mp4");
      },
    },
    {
      name: "NoVideoTrackAndTooManyAudioTracks",
      execute: async () => {
        // Prepare
        spawnSync("cp", ["test_data/two_audios.mp4", GCS_VIDEO_LOCAL_DIR], {
          stdio: "inherit",
        });
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          let statements: Array<Statement> = [
            insertVideoContainerStatement("container1", {
              source: Source.SHOW,
              r2Dirname: "container1",
              totalBytes: 0,
              version: 0,
            }),
            insertVideoTrackStatement("container1", "video1", {
              formatting: {
                gcsFilename: "two_audios.mp4",
              },
            }),
            insertVideoFormattingTaskStatement("container1", "video1", 0, 0),
          ];
          for (let i = 0; i < AUDIO_TRACKS_LIMIT; i++) {
            statements.push(
              insertAudioTrackStatement("container1", `audio${i}`, {}),
            );
          }
          await transaction.batchUpdate(statements);
          await transaction.commit();
        });
        let now = 1000;
        let id = 0;
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          videoId: "video1",
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
              totalBytes: 0,
              version: 0,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data unchanged",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                failure: {
                  reasons: [
                    FailureReason.VIDEO_NEEDS_AT_LEAST_ONE_TRACK,
                    FailureReason.AUDIO_TOO_MANY_TRACKS,
                  ],
                },
              },
            }),
          ]),
          "video track failure",
        );
        assertThat(
          (await getVideoFormattingTasks(SPANNER_DATABASE, 1000000)).length,
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
        await cleanupAll("two_audios.mp4");
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
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
          new BlockingLoopMock(),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          videoId: "video1",
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
              totalBytes: 0,
              version: 0,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data unchanged",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                failure: {
                  reasons: [
                    FailureReason.VIDEO_CODEC_REQUIRES_H264,
                    FailureReason.AUDIO_CODEC_REQUIRES_AAC,
                  ],
                },
              },
            }),
          ]),
          "video track failure",
        );
        assertThat(
          (await getVideoFormattingTasks(SPANNER_DATABASE, 1000000)).length,
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
        await cleanupAll("h265_opus_codec.mp4");
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
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
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
          videoId: "video1",
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
              totalBytes: 0,
              version: 0,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data unchanged",
        );
        assertThat(
          await getAllVideoTracks(SPANNER_DATABASE, "container1"),
          isArray([
            eqGetAllVideoTracksRow({
              videoTrackVideoId: "video1",
              videoTrackData: {
                formatting: {
                  gcsFilename: "one_video_one_audio.mp4",
                },
              },
            }),
          ]),
          "video track unchanged",
        );
        assertThat(
          await getVideoFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoFormattingTasksRow({
              videoFormattingTaskContainerId: "container1",
              videoFormattingTaskVideoId: "video1",
              videoFormattingTaskExecutionTimestamp: 481000,
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
        await cleanupAll("one_video_one_audio.mp4");
      },
    },
    {
      name: "StalledFormatting_DelayingTasksFurther_ResumeButNotInFormattingState",
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
        let handler = new ProcessVideoFormattingTaskHandler(
          SPANNER_DATABASE,
          GCS_VIDEO_LOCAL_DIR,
          R2_VIDEO_LOCAL_DIR,
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
          videoId: "video1",
        });
        await firstEncounter;

        // Verify
        assertThat(
          await getVideoFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoFormattingTasksRow({
              videoFormattingTaskContainerId: "container1",
              videoFormattingTaskVideoId: "video1",
              videoFormattingTaskExecutionTimestamp: 481000,
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
          await getVideoFormattingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqGetVideoFormattingTasksRow({
              videoFormattingTaskContainerId: "container1",
              videoFormattingTaskVideoId: "video1",
              videoFormattingTaskExecutionTimestamp: 580000,
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
            updateVideoTrackStatement(
              {
                failure: {},
              },
              "container1",
              "video1",
            ),
            deleteVideoFormattingTaskStatement("container1", "video1"),
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
              totalBytes: 0,
              version: 0,
            },
            VIDEO_CONTAINER_DATA,
          ),
          "container data unchanged",
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
      },
      tearDown: async () => {
        await cleanupAll("one_video_one_audio.mp4");
      },
    },
  ],
});
