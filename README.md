## Local test setup

### GCP project

Requires a project named as `phading-dev`.

### GCS

Create a bucket named as `phading-dev-video-test` with location at `us-central1 (Iowa)`.

### gcsfuse

Install: https://cloud.google.com/storage/docs/cloud-storage-fuse/install

Mount bucket (https://cloud.google.com/storage/docs/cloud-storage-fuse/mount-bucket):

```sh
gcloud auth application-default login # optional
mkdir gcs_video_test
gcsfuse phading-dev-video-test gcs_video_test
```

### R2

1. Create a bucket named as `video-test` bucket in Cloudflare R2.
1. Create or assign a test API token to the bucket.
1. Roll the API token and save access key ID and secret access key locally. Do not commit to codebase.

### rclone

Install: https://rclone.org/install/#linux

Run `rclone config`:
1. Choose `n` to create new remote.
1. Name `cloudflare_r2`.
1. Choose `4` for amazone s3.
1. Choose `6` for Cloudflare R2.
1. Choose `1` to enter credentials.
1. Copy paste the access key id and secret access key stored above.
1. Choose `1` as auto.
1. Input endpoint `https://0b24a44c8ba9c1914d93e900a9783451.r2.cloudflarestorage.com`.
1. Save and quit.

Then to mount:

```sh
mkdir r2_video_test
rclone mount cloudflare_r2:video-test r2_video_test --vfs-cache-mode=writes & # (It's blocking and has to be run in the background.)
```

Unmount:

```sh
fusermount -u r2_video_test
```

### Spanner

Setup:

```sh
gcloud auth application-default login # optional
gcloud spanner instances create test --config=regional-us-central1 --description="test" --edition=STANDARD --processing-units=100
gcloud spanner databases create test --instance=test
npx spanage update db/ddl -p phading-dev -i test -d test
```

Cleanup:

```sh
gcloud spanner instances delete test
```

### Env vars

```sh
export PROJECT_ID=phading-dev
export INSTANCE_ID=test
export DATABASE_ID=test
export GCS_VIDEO_REMOTE_BUCKET=phading-dev-video-test
export GCS_VIDEO_LOCAL_DIR=gcs_video_test
export R2_VIDEO_REMOTE_BUCKET=video-test
export R2_VIDEO_LOCAL_DIR=r2_video_test
export R2_CONFIG_NAME=cloudflare_r2
```

### FFMPEG

`sudo apt-get install ffmpeg`

## FFMPEG Cheat sheat

Convert to HLS without re-encoding with master playlist and splitted audio tracks: `ffmpeg -i input.mp4 -map 0:v:0 -c:v copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_video_%03d.ts" -master_pl_name output_master.m3u8 output_video.m3u8 -map 0:a:0 -c:a copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_audio0_%03d.ts" output_audio0.m3u8 -map 0:a:1 -c:a copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_audio1_%03d.ts" output_audio1.m3u8`

Get codec of video: `ffprobe -v error -show_entries stream=codec_name,codec_type,width,height,duration -of json input.mp4`

Get audio only: `ffmpeg -i input.mp4 -vn -acodec copy audio.aac`

Get video only: `ffmpeg -i input.mp4 -an -vcodec copy video.mp4`

Add additional audio track to a mp4 video: `ffmpeg -i video.mp4 -i audio.aac -c copy -map 0:v -map 0:a -map 1:a output.mp4`

## TODO

Monitor `rclone mount` health.
