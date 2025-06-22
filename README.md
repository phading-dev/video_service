## Local test dependencies outside of package.json

1. ffmpeg: `sudo apt-get install ffmpeg`
1. unzip: `sudo apt-get install unzip`
1. gcsfuse: https://cloud.google.com/storage/docs/cloud-storage-fuse/install
1. rclone: https://rclone.org/install/

## FFMPEG Cheat sheat

Convert to HLS without re-encoding with master playlist and splitted audio tracks: `ffmpeg -i input.mp4 -map 0:v:0 -c:v copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_video_%03d.ts" -master_pl_name output_master.m3u8 output_video.m3u8 -map 0:a:0 -c:a copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_audio0_%03d.ts" output_audio0.m3u8 -map 0:a:1 -c:a copy -f hls -hls_playlist_type vod -hls_time 6 -hls_segment_filename "o_audio1_%03d.ts" output_audio1.m3u8`

Get codec of video: `ffprobe -v error -show_entries stream=codec_name,codec_type,width,height,duration:stream_tags -of json input.mp4`

Get audio only: `ffmpeg -i input.mp4 -vn -acodec copy audio.aac`

Get video only: `ffmpeg -i input.mp4 -an -vcodec copy video.mp4`

Add additional audio track to a mp4 video: `ffmpeg -i video.mp4 -i audio.aac -c copy -map 0:v -map 0:a -map 1:a output.mp4`
