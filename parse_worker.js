import fillRawTables from "./html_to_raw.js";
import prepareDoc from "./prepare_doc.js";

export default async function parseOneHtml({ html, extra }) {
  const raw = (await fillRawTables(html)) || {};
  console.log("RAW = ", JSON.stringify(raw, null, 2))
  const doc = await prepareDoc(raw);
  console.log("DOC = ", doc)

  // если нужно добавить данные из unit
  if (extra && typeof extra === "object") {
    for (const [k, v] of Object.entries(extra)) {
      if (doc[k] === undefined) doc[k] = v;
    }
  }

  return doc;
}
