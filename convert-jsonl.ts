import axios from "axios";
import * as fs from "fs";
import * as readline from "readline";

export const convertTxtToJsonL = (input: string, output: string) => {
  const totalLines = fs.readFileSync(input, "utf-8");
  const lines = totalLines.split(/\r?\n/);
  const out: string[] = [];
  let logs = 0;
  let headers = 0;
  let errores = 0;

  for (let i = 0; i < lines.length; i++) {
    const header = lines[i].trim();

    // Salta líneas vacías
    if (!header) continue;

    // Solo procesa si encuentra header válido
    if (!header.startsWith("[")) continue;

    const match = header.match(/^\[(.+?)\] \[(.+?)\] (.+)$/);
    if (!match) {
      errores++;
      continue;
    }

    const [, time, level, message] = match;
    headers++;

    // Busca la próxima línea no vacía (puede haber líneas vacías entre header y JSON)
    let j = i + 1;
    let jsonLine = "";

    while (j < lines.length) {
      jsonLine = lines[j].trim();
      if (jsonLine) break;
      j++;
    }

    if (!jsonLine.startsWith("{")) {
      errores++;
      console.error(`Header en línea ${i + 1} SIN JSON después:`, { header });
      continue;
    }

    try {
      const { dt, hostname, pid, ...obj } = JSON.parse(jsonLine);
      const final = { time: dt || time, level, message, ...obj };
      out.push(JSON.stringify(final));
      logs++;
    } catch (e: any) {
      errores++;
      console.error(`JSON inválido después del header en línea ${i + 1}:`, {
        header,
        jsonLine,
        error: e.message,
      });
      continue;
    }

    // Avanza el puntero a después del JSON procesado
    i = j;
  }

  fs.writeFileSync(output, out.join("\n"), "utf-8");
  console.log(`Archivo jsonl listo con ${logs} líneas: ${output}`);
  console.log(`Headers encontrados: ${headers}`);
  console.log(`Errores: ${errores}`);
};

export const importJsonLtoElasticSearch = async (
  filename: string,
  index: string = "eze-logs"
) => {
  const fileStream = fs.createReadStream(filename);

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let count = 0;
  let errors = 0;

  for await (const line of rl) {
    const trimmed = line.trim();

    if (!trimmed) continue;

    try {
      // Opcional: validar que sea JSON válido
      JSON.parse(trimmed);

      await axios.post(`http://localhost:9200/${index}/_doc`, trimmed, {
        headers: {
          "Content-Type": "application/json",
        },
        // timeout: 2000,
      });
      count++;
      if (count % 100 === 0) {
        console.log(`${count} documents imported...`);
      }
    } catch (e: any) {
      errors++;
      console.error(`Error en línea ${count + errors}:`, e.message);
    }
  }

  console.log(`Importación terminada. Exitosos: ${count}, Errores: ${errors}`);
};
