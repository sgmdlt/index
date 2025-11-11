import { load } from "cheerio";

const _text = ($, el) => ($(el).text() || "").replace(/\s+/g, " ").trim();
const _has_nested_table = ($, cell) => $(cell).find("table").first().length > 0;

// Дочерние <tr> текущей таблицы (имитация :scope > tbody > tr, затем :scope > tr)
function tableRows($, table) {
  const tb = $(table).children("tbody");
  if (tb.length) {
    const trs = tb.children("tr");
    if (trs.length) return trs.toArray();
  }
  return $(table).children("tr").toArray();
}

// Дочерние ячейки конкретной строки (имитация :scope > td / :scope > th)
const rowTds = ($, tr) => $(tr).children("td").toArray();
const rowThs = ($, tr) => $(tr).children("th").toArray();

async function parseTable($, tab) {
  const trs_all = tableRows($, tab);
  if (!trs_all.length) return {};

  // 1) Отбрасываем секционные заголовки <th colspan> и пустые строки
  const data_rows = [];
  for (const tr of trs_all) {
    const thColspan = rowThs($, tr).find((th) => $(th).attr("colspan"));
    if (thColspan) continue;
    const tds = rowTds($, tr);
    if (!tds.length) continue;
    data_rows.push(tr);
  }
  if (!data_rows.length) return {};

  // 2) Определяем тип по первой «данной» строке
  const first = data_rows[0];
  const first_tds = rowTds($, first);

  // ---- ВАРИАНТ A: карточка "ДЕЛО" — строго 2 ячейки -> dict
  if (first_tds.length === 2) {
    const result = {};
    for (const tr of data_rows) {
      const tds = rowTds($, tr);
      if (tds.length !== 2) continue;
      const [k_cell, v_cell] = tds;
      const key = _text($, k_cell);
      if (!key) continue;

      if (_has_nested_table($, v_cell)) {
        const nested = $(v_cell).find("table").first();
        result[key] = await parseTable($, nested);
      } else {
        result[key] = _text($, v_cell);
      }
    }
    return result;
  }

  // ---- ВАРИАНТ B: реестры — заголовки по <b> в первой строке-шапке
  let header_tr = null;
  let headers = [];

  for (const tr of data_rows) {
    const bolds = $(tr).children("td").children("b").toArray();
    if (bolds.length >= 2) {
      header_tr = tr;
      headers = bolds.map((b) => _text($, b));
      break;
    }
  }

  // Если жирных нет — берём текст ячеек первой data-строки
  if (!header_tr) {
    header_tr = first;
    headers = first_tds.map((td) => _text($, td));
  }

  // Собираем строки данных после header_tr
  const rows = [];
  let header_seen = false;
  for (const tr of data_rows) {
    if (!header_seen) {
      if (tr === header_tr) header_seen = true;
      continue; // пропускаем строку-шапку
    }
    const tds = rowTds($, tr);
    if (!tds.length) continue;

    const row = {};
    for (let i = 0; i < headers.length; i++) {
      const k = headers[i];
      if (!k) continue;
      const v = tds[i];
      if (!v) continue;

      if (_has_nested_table($, v)) {
        const nested = $(v).find("table").first();
        row[k] = await parseTable($, nested);
      } else {
        row[k] = _text($, v);
      }
    }
    if (Object.keys(row).length) rows.push(row);
  }

  return rows;
}

function extractVerdicts($) {
  const cont5 = $("#cont5");
  if (!cont5.length) return null;

  const out = {};

  // Внутри: ul.tabs > li (названия), рядом .contentt с div[id^=cont_doc]
  const liTabs = cont5.find("ul.tabs > li").toArray();
  const contentBlocks = cont5.find(".contentt > div[id^=cont_doc]").toArray();

  if (liTabs.length && contentBlocks.length) {
    // Если количество совпадает — сводим по парам
    for (let i = 0; i < Math.min(liTabs.length, contentBlocks.length); i++) {
      const name = _text($, liTabs[i]); // например: "Судебный акт #1 (Приговор)"
      const txt = _text($, contentBlocks[i]); // чистый текст акта
      out[name] = txt;
    }
  } else {
    // fallback: если один документ без явной вкладки
    const single = cont5.find(".contentt > div[id^=cont_doc]").first();
    if (single.length) {
      out["Судебный акт"] = _text($, single);
    }
  }

  return out;
}

export default async function fillRawTables(html) {
  html = html.replace(/<\/th>\s+<tr>\s+<tr>/gi, "</th></tr><tr>");

  const $ = load(html, {
    decodeEntities: true,
    xmlMode: false,
    lowerCaseTags: false,
  });

  const formatted_case = {};
  formatted_case["raw_tables"] = {};

  const tables = $("table").toArray();

  if (tables.length === 1) {
    const th = $(tables[0]).find("tr").first().find("th").first();
    const name = th.length ? _text($, th) : "Таблица";
    formatted_case["raw_tables"][name] = await parseTable($, tables[0]);
  } else {
    for (const table of tables) {
      const th = $(table).find("tr").first().find("th").first();
      const table_name = th.length ? _text($, th) : "Таблица";
      const content = await parseTable($, table);
      formatted_case["raw_tables"][table_name] = content;
    }
  }

  const verdict = extractVerdicts($);
  formatted_case["raw_tables"] = {
    ...formatted_case["raw_tables"],
    ...verdict,
  };

  return formatted_case;
}
