import * as fs from "node:fs";
import * as path from "node:path";
import { format } from "date-fns/format";
import * as cron from "node-cron";
import * as Databases from "./databases.json";
import { exec, execSync } from "node:child_process";
import { authorize, deleteOldBackups, uploadToDrive } from "./drive";
import { sendAlert } from "./notify";
import { DbConfig } from "./types";
import { drive_v3, google } from "googleapis";

const DUMP_MIN_SIZE_BYTES = 100;
const DUMP_COMPLETED_MARKER = "-- Dump completed";

// Configuration
const BACKUP_FOLDER = "./backups";

const IS_LOCAL = process.env.NODE_ENV === "local";

const databasesConfig = Databases as DbConfig[];

const createAndCompressDump = async (dbName: string): Promise<string> => {
  const DatabaseConfig = databasesConfig.find((db) => db.name === dbName);

  if (!DatabaseConfig) {
    console.error(`Config for ${dbName} not found`);

    return "";
  }

  const timestamp = format(new Date(), "yyyy_MM_dd_HH_mm_ss");
  const backupFileName = `${DatabaseConfig.name}_backup_${timestamp}.sql`;
  const backupFilePath = path.join(BACKUP_FOLDER, backupFileName);
  const compressedFilePath = `${backupFilePath}.gz`;

  // Comando mysqldump
  const escapedPassword = DatabaseConfig.password.replace(/'/g, "'\\''");
  const dumpCommand = `mysqldump --column-statistics=0 -h ${DatabaseConfig.host} --port=${DatabaseConfig.port} -u ${DatabaseConfig.user} -p'${escapedPassword}' --single-transaction --lock-tables=false ${DatabaseConfig.database} | gzip > ${compressedFilePath}`;

  console.log(`Running mysqldump command`);

  return new Promise((resolve, reject) => {
    exec(dumpCommand, (error, stdout, stderr) => {
      if (error) {
        console.error("Error creating the dump:", error);
        reject(error);

        return;
      }

      console.log(
        "Dump successfully created and compressed at:",
        compressedFilePath,
      );

      // Devuelve la ruta del archivo comprimido
      resolve(compressedFilePath);
    });
  });
};

const validateDump = (
  compressedFilePath: string,
  dbConfig: DbConfig,
): string | null => {
  console.log(`Validating dump: ${compressedFilePath}`);

  // 1. Check file size
  const stats = fs.statSync(compressedFilePath);

  if (stats.size < DUMP_MIN_SIZE_BYTES) {
    return `File too small (${stats.size} bytes). Dump may be empty or corrupted.`;
  }

  // 2. Check gzip integrity
  try {
    execSync(`gzip -t ${compressedFilePath}`);
  } catch {
    return "Gzip integrity check failed. File is corrupted.";
  }

  // 3. Check mysqldump completion marker
  try {
    const tail = execSync(
      `gzip -dc ${compressedFilePath} | tail -c 500`,
    ).toString();

    if (!tail.includes(DUMP_COMPLETED_MARKER)) {
      return "Dump completion marker not found. The dump may have been interrupted.";
    }
  } catch {
    return "Could not read dump contents.";
  }

  // 4. Check minimum expected tables
  if (dbConfig.minTables) {
    try {
      const countOutput = execSync(
        `gzip -dc ${compressedFilePath} | grep -c "CREATE TABLE"`,
      )
        .toString()
        .trim();

      const tableCount = Number.parseInt(countOutput, 10);

      if (tableCount < dbConfig.minTables) {
        return `Found ${tableCount} tables, expected at least ${dbConfig.minTables}. Database may have been compromised.`;
      }

      console.log(
        `Found ${tableCount} tables (minimum: ${dbConfig.minTables})`,
      );
    } catch {
      return "Could not count tables in dump.";
    }
  }

  console.log("Dump validation passed");
  return null;
};

const preflightCheck = async (
  dbConfig: DbConfig,
  auth: drive_v3.Options["auth"],
): Promise<boolean> => {
  let ok = true;

  // 1. Check DB connection
  try {
    execSync(
      `mysqladmin ping -h ${dbConfig.host} --port=${dbConfig.port} -u ${dbConfig.user} -p'${dbConfig.password.replace(/'/g, "'\\''")}'`,
      { timeout: 10000 },
    );
    console.log(`[${dbConfig.name}] ✓ Database connection OK`);
  } catch {
    console.error(`[${dbConfig.name}] ✗ Database connection FAILED`);
    await sendAlert(
      `Preflight failed: ${dbConfig.name}`,
      `Database: ${dbConfig.name}\nHost: ${dbConfig.host}\nError: Could not connect to database`,
    );
    ok = false;
  }

  // 2. Check Drive folder access
  try {
    const drive = google.drive({ version: "v3", auth });
    await drive.files.list({
      q: `'${dbConfig.folderId}' in parents`,
      fields: "files(id)",
      pageSize: 1,
    });
    console.log(`[${dbConfig.name}] ✓ Drive folder access OK`);
  } catch {
    console.error(`[${dbConfig.name}] ✗ Drive folder access FAILED`);
    await sendAlert(
      `Preflight failed: ${dbConfig.name}`,
      `Database: ${dbConfig.name}\nFolder ID: ${dbConfig.folderId}\nError: Could not access Google Drive folder`,
    );
    ok = false;
  }

  return ok;
};

const main = async () => {
  for (const dbConfig of databasesConfig) {
    const auth = await authorize(dbConfig);

    if (IS_LOCAL) {
      console.log(`Running dump for ${dbConfig.name}`, dbConfig);

      let backupFilePath = "";

      try {
        backupFilePath = await createAndCompressDump(dbConfig.name);

        console.log(`File created ${backupFilePath}`);

        const validationError = validateDump(backupFilePath, dbConfig);

        if (validationError) {
          console.error(
            `Dump validation failed for ${dbConfig.name}: ${validationError}`,
          );
          await sendAlert(
            `Validation failed: ${dbConfig.name}`,
            `Database: ${dbConfig.name}\nHost: ${dbConfig.host}\nError: ${validationError}\n\nThe dump was NOT uploaded. Old backups were preserved.`,
          );
          continue;
        }

        await uploadToDrive(auth, [backupFilePath], dbConfig.folderId);

        await deleteOldBackups(
          auth,
          dbConfig.folderId,
          dbConfig.name,
          dbConfig.maxDumpLimit,
        );

        console.log(
          "Backup created and uploaded successfully. Old backups cleaned up",
        );
      } catch (error) {
        console.error("Error during backup and upload process:", error);
        await sendAlert(
          `Backup error: ${dbConfig.name}`,
          `Database: ${dbConfig.name}\nHost: ${dbConfig.host}\nError: ${error}`,
        );
      } finally {
        if (backupFilePath && fs.existsSync(backupFilePath)) {
          console.log(`Deleting local file: ${backupFilePath}`);
          fs.rmSync(backupFilePath);
          console.log("Local file deleted");
        }
      }
    } else {
      console.log(`Creating cron for ${dbConfig.name}`, dbConfig);

      const preflightOk = await preflightCheck(dbConfig, auth);

      if (!preflightOk) {
        console.error(`Skipping cron for ${dbConfig.name} due to preflight failure`);
        continue;
      }

      cron.schedule(
        dbConfig.cronTime,
        async () => {
          console.log(`Running cron task for ${dbConfig.name}`, dbConfig);

          let backupFilePath = "";

          try {
            backupFilePath = await createAndCompressDump(dbConfig.name);

            console.log(`File created ${backupFilePath}`);

            const validationError = validateDump(backupFilePath, dbConfig);

            if (validationError) {
              console.error(
                `Dump validation failed for ${dbConfig.name}: ${validationError}`,
              );
              await sendAlert(
                `Validation failed: ${dbConfig.name}`,
                `Database: ${dbConfig.name}\nHost: ${dbConfig.host}\nError: ${validationError}\n\nThe dump was NOT uploaded. Old backups were preserved.`,
              );
              return;
            }

            await uploadToDrive(auth, [backupFilePath], dbConfig.folderId);

            await deleteOldBackups(
              auth,
              dbConfig.folderId,
              dbConfig.name,
              dbConfig.maxDumpLimit,
            );

            console.log(
              "Backup created and uploaded successfully. Old backups cleaned up",
            );
          } catch (error) {
            console.error("Error during backup and upload process:", error);
            await sendAlert(
              `Backup error: ${dbConfig.name}`,
              `Database: ${dbConfig.name}\nHost: ${dbConfig.host}\nError: ${error}`,
            );
          } finally {
            if (backupFilePath && fs.existsSync(backupFilePath)) {
              console.log("Deleting local file:", backupFilePath);
              fs.rmSync(backupFilePath);
              console.log("Local file deleted");
            }
          }
        },
        {
          name: `backup-${dbConfig.name}`,
        },
      );
    }
  }
};

main();
