import * as fs from "fs";

const infile = "./logs/daruma-api-2025-07-14-08.txt";
const outfile = "./jsonl/daruma-api-2025-07-14-08.jsonl";

const lines = fs.readFileSync(infile, "utf-8").split("\n");
const out: string[] = [];

for (let i = 0; i < lines.length; i++) {
  const header = lines[i].trim();
  if (!header.startsWith("[")) continue;
  const match = header.match(/^\[(.+?)\] \[(.+?)\] (.+)$/);
  if (!match) continue;
  const [, time, level, message] = match;
  const jsonLine = lines[i + 1]?.trim();
  if (!jsonLine?.startsWith("{")) continue;
  try {
    const obj = JSON.parse(jsonLine);
    const final = { time, level, message, ...obj };
    out.push(JSON.stringify(final));
  } catch (e) {
    continue;
  }
}

fs.writeFileSync(outfile, out.join("\n"), "utf-8");

console.log("Archivo jsonl listo:", outfile);
