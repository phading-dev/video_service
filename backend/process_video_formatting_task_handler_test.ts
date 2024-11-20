import { spawn } from "child_process";
import { CLOUD_STORAGE } from "../common/cloud_storage";

async function main() {
  let cp = spawn("ffprobe", [
    "-v",
    "error",
    "-select_streams",
    "v:0",
    "-show_entries",
    "stream=codec_name",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    "test_data/video2.mov",
  ]);
  cp.stdout.on('data', (data) => {
    let codec = data.toString().trim();
    console.log(codec, codec.length);
  });
  await new Promise<void>((resolve) => cp.on('exit', resolve));

  cp = spawn('ffprobe', [
    "-v",
    "error",
    "-select_streams",
    "a",
    "-show_entries",
    "stream=codec_name",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    "test_data/two_audio.mp4",
  ]);
  cp.stdout.on('data', (data) => {
    let lines = (data.toString() as string).trim();
    lines.split('\n').map((line) => {
      let codec = line.trim();
      console.log(codec, codec.length)
    });
  });
  await new Promise<void>((resolve) => cp.on('exit', resolve));

  let url = CLOUD_STORAGE.bucket('phading-dev-video-test-bucket').file('video.mp4').getSignedUrl({
    action: 'read',
    expires: Date.now() + 5 * 60 * 1000
  });
  console.log(url)
}

main();
