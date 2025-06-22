import "../local/env";
import { GcsDirWritesVerifier } from "../common/gcs_dir_writes_verifier";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { STORAGE_CLIENT } from "../common/storage_client";
import { VideoContainer } from "../db/schema";
import {
  GET_GCS_KEY_DELETING_TASK_ROW,
  GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
  GET_MEDIA_UPLOADING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsKeyDeletingTaskStatement,
  deleteGcsKeyStatement,
  deleteMediaFormattingTaskStatement,
  deleteMediaUploadingTaskStatement,
  deleteVideoContainerStatement,
  getGcsKey,
  getGcsKeyDeletingTask,
  getMediaFormattingTaskMetadata,
  getMediaUploadingTask,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  listPendingMediaFormattingTasks,
  listPendingMediaUploadingTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessMediaFormattingTaskHandler } from "./process_media_formatting_task_handler";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/last_processing_failure";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { copyFile, rm } from "fs/promises";

let ONE_MONTH_MS = 30 * 24 * 60 * 60 * 1000;
let TWO_YEAR_MS = 2 * 365 * 24 * 60 * 60 * 1000;
let ALL_TEST_GCS_FILE = [
  "h265_opus_codec.mp4",
  "one_video_one_audio.mp4",
  "two_audios.mp4",
  "two_videos_two_audios.mp4",
  "invalid.txt",
];

async function insertVideoContainer(
  videoContainerData: VideoContainer,
): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        accountId: "account1",
        data: videoContainerData,
      }),
      ...(videoContainerData.processing?.mediaFormatting
        ? [
            insertMediaFormattingTaskStatement({
              containerId: "container1",
              gcsFilename:
                videoContainerData.processing.mediaFormatting.gcsFilename,
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]
        : []),
    ]);
    await transaction.commit();
  });
}

function createProcessMediaFormattingTaskHandler(
  now: () => number,
  getId: () => number,
): ProcessMediaFormattingTaskHandler {
  return new ProcessMediaFormattingTaskHandler(
    GcsDirWritesVerifier.create,
    SPANNER_DATABASE,
    now,
    () => `uuid${getId()}`,
  );
}

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteMediaFormattingTaskStatement({
          mediaFormattingTaskContainerIdEq: "container1",
          mediaFormattingTaskGcsFilenameEq: gcsFilename,
        }),
      ),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsKeyDeletingTaskStatement({
          gcsKeyDeletingTaskKeyEq: gcsFilename,
        }),
      ),
      deleteMediaUploadingTaskStatement({
        mediaUploadingTaskContainerIdEq: "container1",
        mediaUploadingTaskGcsDirnameEq: "ouuid0",
      }),
      deleteGcsKeyStatement({
        gcsKeyKeyEq: "ouuid0",
      }),
      deleteGcsKeyDeletingTaskStatement({
        gcsKeyDeletingTaskKeyEq: "ouuid0",
      }),
    ]);
    await transaction.commit();
  });
  await Promise.all(
    ALL_TEST_GCS_FILE.map((gcsFilename) =>
      rm(`${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`, { force: true }),
    ),
  );
  await rm(`${ENV_VARS.gcsVideoMountedLocalDir}/ouuid0`, {
    recursive: true,
    force: true,
  });
}

TEST_RUNNER.run({
  name: "ProcessMediaFormattingTaskHandlerTest",
  cases: [
    {
      name: "FormatOneVideoTrackTwoAudioTracks",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_videos_two_audios.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_videos_two_audios.mp4`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "two_videos_two_audios.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_videos_two_audios.mp4",
        });

        // Verify
        assertThat(id, eq(4), "ids used");
        let [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0/uuid1",
        });
        assertThat(files.length, eq(43), "video files");
        [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0/uuid2",
        });
        assertThat(files.length, eq(43), "audio 1 files");
        [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0/uuid3",
        });
        assertThat(files.length, eq(43), "audio 2 files");
        videoContainerData.processing = {
          mediaUploading: {
            gcsDirname: "ouuid0",
            gcsVideoDirname: "uuid1",
            videoInfo: {
              durationSec: 240,
              resolution: "640x360",
            },
            gcsAudioDirnames: ["uuid2", "uuid3"],
          },
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await getMediaUploadingTask(SPANNER_DATABASE, {
            mediaUploadingTaskContainerIdEq: "container1",
            mediaUploadingTaskGcsDirnameEq: "ouuid0",
          }),
          isArray([
            eqMessage(
              {
                mediaUploadingTaskContainerId: "container1",
                mediaUploadingTaskGcsDirname: "ouuid0",
                mediaUploadingTaskRetryCount: 0,
                mediaUploadingTaskExecutionTimeMs: 1000,
                mediaUploadingTaskCreatedTimeMs: 1000,
              },
              GET_MEDIA_UPLOADING_TASK_ROW,
            ),
          ]),
          "media uploading tasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "two_videos_two_audios.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "two_videos_two_audios.mp4",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "input gcs key delete tasks",
        );
        assertThat(
          (await getGcsKey(SPANNER_DATABASE, { gcsKeyKeyEq: "ouuid0" })).length,
          eq(1),
          "output gcs dir key exists",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "ouuid0",
          }),
          isArray([]),
          "output gcs dir delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FormatTwoAudios",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_audios.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_audios.mp4`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "two_audios.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_audios.mp4",
        });

        // Verify
        assertThat(id, eq(3), "ids used");
        let [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0/uuid1",
        });
        assertThat(files.length, eq(43), "audio 1 files");
        [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0/uuid2",
        });
        assertThat(files.length, eq(43), "audio 2 files");
        videoContainerData.processing = {
          mediaUploading: {
            gcsDirname: "ouuid0",
            gcsAudioDirnames: ["uuid1", "uuid2"],
          },
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "two_audios.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "two_audios.mp4",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "input gcs key delete tasks",
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
        let videoContainerData: VideoContainer = {
          processing: {
            subtitleFormatting: {},
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsFilename: "one_video_one_audio.mp4",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "Video container container1 is not in media formatting state",
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
      name: "VideoFileInvalid",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/invalid.txt",
          `${ENV_VARS.gcsVideoMountedLocalDir}/invalid.txt`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "invalid.txt",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "invalid.txt",
        });

        // Verify
        let [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0",
        });
        assertThat(files.length, eq(0), "output files");
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailure = {
          reasons: [ProcessingFailureReason.MEDIA_FORMAT_INVALID],
          timeMs: now,
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await listPendingMediaUploadingTasks(SPANNER_DATABASE, {
            mediaUploadingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media uploading tasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "invalid.txt",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "invalid.txt",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "input gcs key delete tasks",
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
        await copyFile(
          "test_data/h265_opus_codec.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/h265_opus_codec.mp4`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "h265_opus_codec.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "h265_opus_codec.mp4",
        });

        // Verify
        let [files] = await STORAGE_CLIENT.bucket(
          ENV_VARS.gcsVideoBucketName,
        ).getFiles({
          prefix: "ouuid0",
        });
        assertThat(files.length, eq(0), "output files");
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailure = {
          reasons: [
            ProcessingFailureReason.VIDEO_CODEC_REQUIRES_H264,
            ProcessingFailureReason.AUDIO_CODEC_REQUIRES_AAC,
          ],
          timeMs: now,
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await listPendingMediaUploadingTasks(SPANNER_DATABASE, {
            mediaUploadingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media uploading tasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "h265_opus_codec.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "h265_opus_codec.mp4",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "input gcs key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FormattingFailed",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/one_video_one_audio.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/one_video_one_audio.mp4`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "one_video_one_audio.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );
        handler.interfereFormat = () => Promise.reject(new Error("fake error"));

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });

        // Verify
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailure = {
          reasons: [ProcessingFailureReason.MEDIA_FORMAT_FAILURE],
          timeMs: now,
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await listPendingMediaUploadingTasks(SPANNER_DATABASE, {
            mediaUploadingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media uploading tasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "one_video_one_audio.mp4",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "input gcs key delete tasks",
        );
        assertThat(
          (await getGcsKey(SPANNER_DATABASE, { gcsKeyKeyEq: "ouuid0" })).length,
          eq(1),
          "output gcs dir key exists",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "ouuid0",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "ouuid0",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "output gcs dir delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledFormatting_ResumeButAnotherFileIsBeingFormatted",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/one_video_one_audio.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/one_video_one_audio.mp4`,
        );
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "one_video_one_audio.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => id++,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((resolve1) => {
          handler.interfereFormat = () => {
            resolve1();
            return new Promise<void>((resolve2) => (stallResolveFn = resolve2));
          };
        });

        // Execute
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });
        await firstEncounter;

        // Verify
        assertThat(
          (await getGcsKey(SPANNER_DATABASE, { gcsKeyKeyEq: "ouuid0" })).length,
          eq(1),
          "output gcs dir key exists",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "ouuid0",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "ouuid0",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: ONE_MONTH_MS + 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "output gcs dir delete tasks",
        );

        // Prepare
        now = 2000;
        videoContainerData.processing = {
          mediaFormatting: {
            gcsFilename: "two_audios.mp4",
          },
        };
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            updateVideoContainerStatement({
              videoContainerContainerIdEq: "container1",
              setData: videoContainerData,
            }),
          ]);
          await transaction.commit();
        });

        // Execute
        stallResolveFn();
        let error = await assertReject(processedPromise);

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "is formatting a different file than one_video_one_audio.mp4",
            ),
          ),
          "error",
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "ouuid0",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "ouuid0",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 302000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "output gcs dir delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ClaimTask",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          processing: {
            mediaFormatting: {
              gcsFilename: "one_video_one_audio.mp4",
            },
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let handler = createProcessMediaFormattingTaskHandler(
          () => now,
          () => 0,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });

        // Verify
        assertThat(
          await getMediaFormattingTaskMetadata(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskRetryCount: 1,
                mediaFormattingTaskExecutionTimeMs: 1801000,
              },
              GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "media formatting tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
