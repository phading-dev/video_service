import { spawn } from "child_process";

export async function spawnAsync(
  context: string,
  cmd: string,
  formattingArgs: Array<string>,
): Promise<string> {
  console.log(`${context} Running "${cmd} ${formattingArgs.join(" ")}"`);
  let cp = spawn(cmd, formattingArgs);
  let stdout = new Array<string>();
  cp.stdout.on("data", (data) => {
    stdout.push(data.toString());
  });
  let stderr = new Array<string>();
  cp.stderr.on("data", (data) => {
    stderr.push(data.toString());
  });
  let code = await new Promise<number>((resolve) =>
    cp.on("close", (code) => resolve(code)),
  );
  if (code !== 0) {
    throw Error(
      `${context} process exited with code ${code} and error: ${stderr.join("")}.`,
    );
  }
  return stdout.join("");
}
