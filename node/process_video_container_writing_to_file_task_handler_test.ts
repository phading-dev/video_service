import "../local/env";
import { LOCAL_MASTER_PLAYLIST_NAME } from "../common/constants";
import { FILE_UPLOADER } from "../common/r2_file_uploader";
import { S3_CLIENT, initS3Client } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  GET_VIDEO_CONTAINER_SYNCING_TASK_ROW,
  GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_METADATA_ROW,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getR2Key,
  getR2KeyDeletingTask,
  getVideoContainer,
  getVideoContainerSyncingTask,
  getVideoContainerWritingToFileTaskMetadata,
  insertVideoContainerStatement,
  insertVideoContainerWritingToFileTaskStatement,
  listPendingR2KeyDeletingTasks,
  listPendingVideoContainerSyncingTasks,
  listPendingVideoContainerWritingToFileTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessVideoContainerWritingToFileTaskHandler } from "./process_video_container_writing_to_file_task_handler";
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

let ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
let TWO_YEAR_MS = 2 * 365 * 24 * 60 * 60 * 1000;

async function insertVideoContainer(videoContainerData: VideoContainer) {
  await S3_CLIENT.val.send(
    new PutObjectCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Key: `root/video1/${LOCAL_MASTER_PLAYLIST_NAME}`,
      Body: createReadStream("test_data/video_only_master.m3u8"),
    }),
  );
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        data: videoContainerData,
      }),
      ...(videoContainerData.masterPlaylist.writingToFile
        ? [
            insertVideoContainerWritingToFileTaskStatement({
              containerId: "container1",
              version: videoContainerData.masterPlaylist.writingToFile.version,
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

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteVideoContainerWritingToFileTaskStatement({
        videoContainerWritingToFileTaskContainerIdEq: "container1",
        videoContainerWritingToFileTaskVersionEq: 1,
      }),
      deleteVideoContainerSyncingTaskStatement({
        videoContainerSyncingTaskContainerIdEq: "container1",
        videoContainerSyncingTaskVersionEq: 1,
      }),
      deleteVideoContainerWritingToFileTaskStatement({
        videoContainerWritingToFileTaskContainerIdEq: "container1",
        videoContainerWritingToFileTaskVersionEq: 2,
      }),
      deleteVideoContainerSyncingTaskStatement({
        videoContainerSyncingTaskContainerIdEq: "container1",
        videoContainerSyncingTaskVersionEq: 2,
      }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid0.m3u8" }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid0.m3u8",
      }),
    ]);
    await transaction.commit();
  });
  await S3_CLIENT.val.send(
    new DeleteObjectCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Key: "root/uuid0.m3u8",
    }),
  );
  await S3_CLIENT.val.send(
    new DeleteObjectCommand({
      Bucket: ENV_VARS.r2VideoBucketName,
      Key: `root/video1/${LOCAL_MASTER_PLAYLIST_NAME}`,
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessVideoContainerWritingToFileTaskHandlerTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "ProcessOneVideoAndTwoAudiosAndTwoSubtitlesAndStagingTracks",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: ["file1"],
              r2DirnamesToDelete: ["dir1"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
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
                  resolution: "1920x1080",
                  totalBytes: 2000000,
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
                totalBytes: 123,
              },
            },
            {
              r2TrackDirname: "audio2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 456,
                },
              },
            },
            {
              r2TrackDirname: "audio3",
              committed: {
                name: "Kor",
                isDefault: false,
                totalBytes: 789,
              },
              staging: {
                toAdd: {
                  name: "Chn",
                  isDefault: false,
                  totalBytes: 101112,
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
                totalBytes: 123,
              },
            },
            {
              r2TrackDirname: "subtitle2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 456,
                },
              },
            },
            {
              r2TrackDirname: "subtitle3",
              committed: {
                name: "Kor",
                isDefault: false,
                totalBytes: 789,
              },
              staging: {
                toAdd: {
                  name: "Chn",
                  isDefault: false,
                  totalBytes: 101112,
                },
              },
            },
          ],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          version: 1,
        });

        // Verify
        assertThat(
          await (
            await S3_CLIENT.val.send(
              new GetObjectCommand({
                Bucket: ENV_VARS.r2VideoBucketName,
                Key: "root/uuid0.m3u8",
              }),
            )
          ).Body.transformToString(),
          eq(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=669168,RESOLUTION=640x360,CODECS="avc1.4d401e"
video1/o.m3u8


#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Eng",DEFAULT=YES,AUTOSELECT=NO,URI="audio1/o.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Kor",DEFAULT=NO,AUTOSELECT=NO,URI="audio3/o.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Eng",DEFAULT=YES,AUTOSELECT=NO,URI="subtitle1/o.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Kor",DEFAULT=NO,AUTOSELECT=NO,URI="subtitle3/o.m3u8"`),
          "master playlist content",
        );
        videoContainerData.masterPlaylist = {
          syncing: {
            r2Filename: "uuid0.m3u8",
            version: 1,
            r2FilenamesToDelete: ["file1"],
            r2DirnamesToDelete: ["dir1"],
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingVideoContainerWritingToFileTasks(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await getVideoContainerSyncingTask(SPANNER_DATABASE, {
            videoContainerSyncingTaskContainerIdEq: "container1",
            videoContainerSyncingTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 1,
                videoContainerSyncingTaskRetryCount: 0,
                videoContainerSyncingTaskExecutionTimeMs: 1000,
                videoContainerSyncingTaskCreatedTimeMs: 1000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASK_ROW,
            ),
          ]),
          "syncing tasks",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0.m3u8" }))
            .length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ProcessOneVideoOnly",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 2,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          version: 2,
        });

        // Verify
        assertThat(
          await (
            await S3_CLIENT.val.send(
              new GetObjectCommand({
                Bucket: ENV_VARS.r2VideoBucketName,
                Key: "root/uuid0.m3u8",
              }),
            )
          ).Body.transformToString(),
          eq(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=669168,RESOLUTION=640x360,CODECS="avc1.4d401e"
video1/o.m3u8

`),
          "master playlist content",
        );
        videoContainerData.masterPlaylist = {
          syncing: {
            r2Filename: "uuid0.m3u8",
            version: 2,
            r2FilenamesToDelete: [],
            r2DirnamesToDelete: [],
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingVideoContainerWritingToFileTasks(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await getVideoContainerSyncingTask(SPANNER_DATABASE, {
            videoContainerSyncingTaskContainerIdEq: "container1",
            videoContainerSyncingTaskVersionEq: 2,
          }),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 2,
                videoContainerSyncingTaskRetryCount: 0,
                videoContainerSyncingTaskExecutionTimeMs: 1000,
                videoContainerSyncingTaskCreatedTimeMs: 1000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASK_ROW,
            ),
          ]),
          "syncing tasks",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0.m3u8" }))
            .length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInWritingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              r2Filename: "uuid0.m3u8",
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            version: 1,
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "container container1 is not in writing to file state",
            ),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "WritingInterruptedUnexpectedly",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );
        handler.interfereFn = () => {
          throw new Error("fake error");
        };

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            version: 1,
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("fake error")), "error");
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerWritingToFileTaskMetadata(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskRetryCount: 0,
                videoContainerWritingToFileTaskExecutionTimeMs: 0,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_METADATA_ROW,
            ),
          ]),
          "writing to file tasks",
        );
        assertThat(
          await listPendingVideoContainerSyncingTasks(SPANNER_DATABASE, {
            videoContainerSyncingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "syncing tasks",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0.m3u8" }))
            .length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0.m3u8",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 301000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledWriting_ResumeButVersionChanged",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let now = 1000;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((firstEncounterResolve) => {
          handler.interfereFn = () => {
            firstEncounterResolve();
            return new Promise<void>(
              (stallResolve) => (stallResolveFn = stallResolve),
            );
          };
        });

        // Execute
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          version: 1,
        });
        await firstEncounter;

        // Verify
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0.m3u8" }))
            .length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0.m3u8",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: ONE_YEAR_MS + 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete tasks",
        );

        // Prepare
        now = 2000;
        videoContainerData.masterPlaylist.writingToFile.version = 2;
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
              "is writing to file with a different version than 1",
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerWritingToFileTaskMetadata(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskRetryCount: 0,
                videoContainerWritingToFileTaskExecutionTimeMs: 0,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_METADATA_ROW,
            ),
          ]),
          "remained writing to file tasks",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0.m3u8",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 302000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "remained r2 key delete tasks",
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
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          undefined,
          undefined,
          () => 1000,
          undefined,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          version: 1,
        });

        // Verify
        assertThat(
          await getVideoContainerWritingToFileTaskMetadata(SPANNER_DATABASE, {
            videoContainerWritingToFileTaskContainerIdEq: "container1",
            videoContainerWritingToFileTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskRetryCount: 1,
                videoContainerWritingToFileTaskExecutionTimeMs: 301000,
              },
              GET_VIDEO_CONTAINER_WRITING_TO_FILE_TASK_METADATA_ROW,
            ),
          ]),
          "writing to file task metadata",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
