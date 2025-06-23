import http = require("http");
import { initS3Client } from "./common/s3_client";
import { configureRclone } from "./configure_rclone";
import { ENV_VARS } from "./env_vars";
import { CancelMediaFormattingHandler } from "./node/cancel_media_formatting_handler";
import { CancelSubtitleFormattingHandler } from "./node/cancel_subtitle_formatting_handler";
import { CancelUploadingHandler } from "./node/cancel_uploading_handler";
import { CommitVideoContainerStagingDataHandler } from "./node/commit_video_container_staging_data_handler";
import { CompleteUploadingHandler } from "./node/complete_uploading_handler";
import { CreateVideoContainerHandler } from "./node/create_video_container_handler";
import { DeleteVideoContainerHandler } from "./node/delete_video_container_handler";
import { GetVideoContainerHandler } from "./node/get_video_container_handler";
import { ListGcsKeyDeletingTasksHandler } from "./node/list_gcs_key_deleting_tasks_handler";
import { ListGcsUploadFileDeletingTasksHandler } from "./node/list_gcs_upload_file_deleting_tasks_handler";
import { ListMediaFormattingTasksHandler } from "./node/list_media_formatting_tasks_handler";
import { ListMediaUploadingTasksHandler } from "./node/list_media_uploading_tasks_handler";
import { ListR2KeyDeletingTasksHandler } from "./node/list_r2_key_deleting_tasks_handler";
import { ListStorageEndRecordingTasksHandler } from "./node/list_storage_end_recording_tasks_handler";
import { ListStorageStartRecordingTasksHandler } from "./node/list_storage_start_recording_tasks_handler";
import { ListSubtitleFormattingTasksHandler } from "./node/list_subtitle_formatting_tasks_handler";
import { ListUploadedRecordingTasksHandler } from "./node/list_uploaded_recording_tasks_handler";
import { ListVideoContainerSyncingTasksHandler } from "./node/list_video_container_syncing_tasks_handler";
import { ListVideoContainerWritingToFileTasksHandler } from "./node/list_video_container_writing_to_file_tasks_handler";
import { ProcessGcsKeyDeletingTaskHandler } from "./node/process_gcs_key_deleting_task_handler";
import { ProcessGcsUploadFileDeletingTaskHandler } from "./node/process_gcs_upload_file_deleting_task_handler";
import { ProcessMediaFormattingTaskHandler } from "./node/process_media_formatting_task_handler";
import { ProcessMediaUploadingTaskHandler } from "./node/process_media_uploading_tasks_handler";
import { ProcessR2KeyDeleteHandler } from "./node/process_r2_key_deleting_task_handler";
import { ProcessStorageEndRecordingTaskHandler } from "./node/process_storage_end_recording_task_handler";
import { ProcessStorageStartRecordingTaskHandler } from "./node/process_storage_start_recording_task_handler";
import { ProcessSubtitleFormattingTaskHandler } from "./node/process_subtitle_formatting_task_handler";
import { ProcessUploadedRecordingTaskHandler } from "./node/process_uploaded_recording_task_handler";
import { ProcessVideoContainerSyncingTaskHandler } from "./node/process_video_container_syncing_task_handler";
import { ProcessVideoContainerWritingToFileTaskHandler } from "./node/process_video_container_writing_to_file_task_handler";
import { SaveVideoContainerStagingDataHandler } from "./node/save_video_container_staging_data_handler";
import { StartUploadingHandler } from "./node/start_uploading_handler";
import { VIDEO_NODE_SERVICE } from "@phading/video_service_interface/service";
import { ServiceHandler } from "@selfage/service_handler/service_handler";

async function main() {
  await Promise.all([initS3Client(), configureRclone()]);
  let service = ServiceHandler.create(
    http.createServer(),
    ENV_VARS.externalOrigin,
  )
    .addCorsAllowedPreflightHandler()
    .addHealthCheckHandler()
    .addReadinessHandler()
    .addMetricsHandler();
  service
    .addHandlerRegister(VIDEO_NODE_SERVICE)
    .add(CancelMediaFormattingHandler.create())
    .add(CancelSubtitleFormattingHandler.create())
    .add(CancelUploadingHandler.create())
    .add(CommitVideoContainerStagingDataHandler.create())
    .add(CompleteUploadingHandler.create())
    .add(CreateVideoContainerHandler.create())
    .add(DeleteVideoContainerHandler.create())
    .add(GetVideoContainerHandler.create())
    .add(ListGcsKeyDeletingTasksHandler.create())
    .add(ListGcsUploadFileDeletingTasksHandler.create())
    .add(ListMediaFormattingTasksHandler.create())
    .add(ListMediaUploadingTasksHandler.create())
    .add(ListR2KeyDeletingTasksHandler.create())
    .add(ListStorageEndRecordingTasksHandler.create())
    .add(ListStorageStartRecordingTasksHandler.create())
    .add(ListSubtitleFormattingTasksHandler.create())
    .add(ListUploadedRecordingTasksHandler.create())
    .add(ListVideoContainerSyncingTasksHandler.create())
    .add(ListVideoContainerWritingToFileTasksHandler.create())
    .add(ProcessGcsKeyDeletingTaskHandler.create())
    .add(ProcessGcsUploadFileDeletingTaskHandler.create())
    .add(ProcessMediaFormattingTaskHandler.create())
    .add(ProcessMediaUploadingTaskHandler.create())
    .add(ProcessR2KeyDeleteHandler.create())
    .add(ProcessStorageEndRecordingTaskHandler.create())
    .add(ProcessStorageStartRecordingTaskHandler.create())
    .add(ProcessSubtitleFormattingTaskHandler.create())
    .add(ProcessUploadedRecordingTaskHandler.create())
    .add(ProcessVideoContainerSyncingTaskHandler.create())
    .add(ProcessVideoContainerWritingToFileTaskHandler.create())
    .add(SaveVideoContainerStagingDataHandler.create())
    .add(StartUploadingHandler.create());
  await service.start(ENV_VARS.port);
}

main();
