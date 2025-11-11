import minimist from "minimist";

const argv = minimist(process.argv.slice(2));

import { Client } from "@elastic/elasticsearch";
import streamChain from "stream-chain";
import fs from "fs";
import JSONLparser from "stream-json/jsonl/Parser.js";
import prepareDoc from "./prepare_doc.js";
import cliProgress from "cli-progress";
import { execSync } from "child_process";

const progressBar = new cliProgress.SingleBar(
  {
    format: "Progress [{bar}] {percentage}% | {value}/{total} lines",
    hideCursor: true,
  },
  cliProgress.Presets.shades_classic
);

process.on("SIGINT", function () {
  console.log("Caught interrupt signal");

  progressBar.stop();

  process.exit();
});

const path = argv.dir;
const file = argv.file;
const tag = argv.tag;
const indexName = argv.index;
const start = argv.start;

const esClient = new Client({
  node: "http://localhost:9200", // Elasticsearch endpoint
  auth: {
    apiKey: "M21GXzlKTUJVamZ4N3RyaVFZS2M6d3IwbUpzUnlSSXVGd3U3SVpvZE1qUQ==",
  },
  requestTimeout: 120000,
});

console.log("Start processing files in path: ", path);
let files;
let prefix = "";
if (path) {
  files = fs.readdirSync(path);
  prefix = path + "/";
} else if (file) {
  files = [file];
}

for (const file of files) {
  if (start && file < start) {
    continue;
  }
  console.log("Processing file: ", file);

  const filePath = `${prefix}${file}`;

  // Get total lines using `wc -l`
  let totalLines = 0;
  try {
    totalLines = parseInt(
      execSync(`wc -l < "${filePath}"`, { encoding: "utf8" }).trim(),
      10
    );
  } catch (error) {
    console.error(`Error counting lines in ${file}:`, error);
    continue;
  }

  if (totalLines > 0) {
    progressBar.start(totalLines, 0);
  }

  let processedLines = 0;

  const stream = fs.createReadStream(`${prefix}${file}`);
  const pipeline = streamChain.chain([stream, JSONLparser.parser()]);

  const result = await esClient.helpers.bulk({
    datasource: pipeline,
    retries: 10,
    wait: 60000,
    onDocument(doc) {
      const obj = prepareDoc(doc.value);

      processedLines++;
      progressBar.update(processedLines);

      if (tag) {
        obj.tag = tag;
      }
      return [{ index: { _index: indexName, _id: doc.value.id_final } }, obj];
    },
    onDrop(doc) {
      console.log(doc);
    },
  });

  progressBar.stop();
}
