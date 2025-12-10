import fsp from "fs/promises";
import path from "path";

const normPath = (p) => String(p).replace(/\\/g, "/");

async function isDir(p) {
  try {
    return (await fsp.stat(p)).isDirectory();
  } catch {
    return false;
  }
}

async function readDirSafe(p) {
  try {
    return await fsp.readdir(p, { withFileTypes: true });
  } catch {
    return [];
  }
}

async function readJsonSafe(file) {
  try {
    return JSON.parse(await fsp.readFile(file, "utf8"));
  } catch (e) {
    console.warn(`Пропуск (невалидный JSON): ${file}: ${e.message}`);
    return null;
  }
}

async function listHtmlInDir(dir) {
  const names = await readDirSafe(dir);

  function parseNameToTimestamp(name) {
    const fileName = name.toLowerCase().endsWith(".html")
      ? name.slice(0, -5)
      : name;

    const [datePart, timePart] = fileName.split(".");
    if (!datePart || !timePart) return NaN;

    const [dayStr, monthStr, yearStr] = datePart.split("-");
    const [hourStr, minuteStr, secondStr] = timePart.split(":");

    const day = Number(dayStr);
    const month = Number(monthStr);
    const year = Number(yearStr);
    const hour = Number(hourStr);
    const minute = Number(minuteStr);
    const second = Number(secondStr);

    if (
      Number.isNaN(day) || Number.isNaN(month) || Number.isNaN(year) ||
      Number.isNaN(hour) || Number.isNaN(minute) || Number.isNaN(second)
    ) {
      return NaN;
    }

    return Date.UTC(year, month - 1, day, hour, minute, second);
  }

  const html = names
    .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".html"))
    .map((e) => e.name);

  const withTs = html.map((n) => ({ n, ts: parseNameToTimestamp(n) }));
  withTs.sort((a, b) => a.ts - b.ts);

  return withTs.map((x) => x.n);
}

/**
 * Асинхронный генератор юнитов.
 * @param {string} root Абсолютный или относительный путь к корню.
 */
export async function* traverseCases(root) {
  const rootAbs = path.resolve(root);
  if (!(await isDir(rootAbs))) {
    throw new Error(
      `--root должен указывать на существующий каталог: ${rootAbs}`
    );
  }

  // DFS без рекурсии (чтобы не упираться в глубину стека)
  const stack = [rootAbs];

  while (stack.length) {
    const cur = stack.pop();
    const entries = await readDirSafe(cur);

    // Сначала пушим подкаталоги (для стабильности — в обратном алфавите, чтобы итерация была по возрастанию)
    const dirs = entries
      .filter((e) => e.isDirectory())
      .map((e) => path.join(cur, e.name))
      .sort()
      .reverse();
    for (const d of dirs) stack.push(d);

    // Проверяем наличие manifest.json в текущей папке
    const hasManifest = entries.some(
      (e) => e.isFile() && e.name === "manifest.json"
    );
    if (!hasManifest) continue;

    const manifestPath = path.join(cur, "manifest.json");
    const meta = await readJsonSafe(manifestPath);
    if (!meta) continue; // невалидный JSON — пропускаем

    const htmlFiles = await listHtmlInDir(cur);
    const relFromRoot = normPath(path.relative(rootAbs, manifestPath));

    yield {
      manifestPath: normPath(manifestPath),
      relFromRoot,
      meta,
      htmlFiles,
    };
  }
}
