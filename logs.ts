import {
  format,
  parse,
  isValid,
  subDays,
  setHours,
  startOfHour,
  endOfHour,
} from "date-fns";
import * as fs from "fs";
import * as LogConfigs from "./sources.json";
import axios from "axios";
import { authorize, deleteOldBackups, uploadToDrive } from "./drive";
import * as cron from "node-cron";
import { LogConfig } from "./types";
import { convertTxtToJsonL } from "./convert-jsonl";

// Configuration
const BACKUP_FOLDER = "./logs";
const MAX_DUMP_LIMIT = 2;

const SCOPES = ["https://www.googleapis.com/auth/drive.file"];

const IS_LOCAL = process.env.NODE_ENV === "local";

const ENDPOINT = "https://telemetry.betterstack.com/api/v2/query/explore-logs";

function getBaseDate(dateArg?: string): Date {
  if (dateArg) {
    const parsed = parse(dateArg, "yyyy-MM-dd", new Date());
    if (!isValid(parsed))
      throw new Error("Fecha inválida. Usa formato YYYY-MM-DD");
    return parsed;
  } else {
    return subDays(new Date(), 1);
  }
}

async function fetchLogsChunk(
  sourceId: string,
  apiKey: string,
  from: string,
  to: string,
  limit: number,
  offset: number
): Promise<any[]> {
  const query = `SELECT {{time}} as time, JSONExtract(json, 'level', 'Nullable(String)') AS level, JSONExtract(json, 'message', 'Nullable(String)') AS message, json FROM {{source}} WHERE time BETWEEN {{start_time}} AND {{end_time}} ORDER BY {{time}} ASC LIMIT ${limit} OFFSET ${offset} FORMAT JSON`;

  const params = new URLSearchParams();
  params.append("source_ids", sourceId);
  params.append("query", query);
  params.append("from", from);
  params.append("to", to);

  const { data } = await axios.post(ENDPOINT, params.toString(), {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    responseType: "json",
  });

  return data.data || [];
}

const createDump = async (logConfig: LogConfig) => {
  const dateArg = process.argv[2];
  const baseDate = getBaseDate(dateArg);
  const dayString = format(baseDate, "yyyy-MM-dd");
  const files: string[] = [];

  const limit = 1000;

  for (let hour = 0; hour < 24; hour++) {
    const fromDate = setHours(startOfHour(baseDate), hour);
    const toDate = setHours(endOfHour(baseDate), hour);

    const from = format(fromDate, "yyyy-MM-dd HH:mm:ss");
    const to = format(toDate, "yyyy-MM-dd HH:mm:ss");
    const filename = `./logs/${logConfig.name}-${dayString}-${format(
      fromDate,
      "HH"
    )}.txt`;

    // SALTEA si ya existe
    if (fs.existsSync(filename)) {
      console.log(`Ya existe: ${filename} → salteando`);
      files.push(filename);
      continue;
    }

    const stream = fs.createWriteStream(filename, {
      flags: "w",
      encoding: "utf-8",
    });

    let offset = 0;
    let fetched = 0;
    let chunk: any[] = [];
    let logsThisHour = 0;

    console.log(
      `Descargando logs de ${dayString} hora ${format(fromDate, "HH")}...`
    );

    do {
      try {
        chunk = await fetchLogsChunk(
          logConfig.sourceId,
          logConfig.apiKey,
          from,
          to,
          limit,
          offset
        );

        if (chunk.length === 0) break;

        for (const log of chunk) {
          stream.write(
            `[${log.time}] [${log.level}] ${log.message}\n${log.json}\n\n`
          );

          logsThisHour++;
        }

        fetched += chunk.length;
        offset += limit;
      } catch (err: any) {
        if (err.response) {
          console.error("Error response:", err.response.data);
        } else {
          console.error("Error:", err.message);
        }

        stream.end();
        process.exit(1);
      }
    } while (chunk.length === limit);

    stream.end(() => {
      if (logsThisHour > 0) {
        console.log(`  → Guardados ${logsThisHour} logs en ${filename}`);
      } else {
        if (!IS_LOCAL) {
          fs.unlinkSync(filename);
        }
        console.log("  → Sin logs en este rango horario.");
      }

      const finalFileName = `${filename.replace(".txt", ".jsonl")}`;

      convertTxtToJsonL(filename, finalFileName);

      if (!IS_LOCAL) {
        fs.unlinkSync(filename);
      }

      files.push(filename);
    });

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  return files;
};

async function main() {
  for (const logConfig of LogConfigs) {
    const auth = await authorize(logConfig);

    if (!IS_LOCAL) {
      console.log(`Creating cron for ${logConfig.name}`, logConfig);

      cron.schedule(
        logConfig.cronTime,
        async () => {
          console.log(`Running cron task for ${logConfig.name}`, logConfig);

          let backupFilePath: string[] = [];

          try {
            backupFilePath = await createDump(logConfig);

            console.log(`Files created \r\n${backupFilePath.join("\r\n")}`);

            await uploadToDrive(auth, backupFilePath, logConfig.folderId);

            await deleteOldBackups(
              auth,
              logConfig.folderId,
              logConfig.name,
              logConfig.maxDumpLimit
            );

            console.log(
              "Backup created and uploaded successfully. Old backups cleaned up"
            );
          } catch (error) {
            console.error("Error during backup and upload process:", error);
          }
        },
        {
          name: `backup-logs-${logConfig.name}`,
        }
      );
    } else {
      let backupFilePaths: string[] = [];

      try {
        backupFilePaths = await createDump(logConfig);

        console.log(`Files created \r\n${backupFilePaths.join("\r\n")}`);

        await uploadToDrive(auth, backupFilePaths, logConfig.folderId);

        await deleteOldBackups(
          auth,
          logConfig.folderId,
          logConfig.name,
          logConfig.maxDumpLimit
        );

        console.log(
          "Backup created and uploaded successfully. Old backups cleaned up"
        );
      } catch (error) {
        console.error("Error during backup and upload process:", error);
      }
    }

    console.log("¡Listo! Logs descargados por hora.");
  }
}

main();
