import { DuneClient, QueryParameter } from "@cowprotocol/ts-dune-client";
import "dotenv/config";
import fs from "fs";
import path from "path";

const START_DATE = "2023-04-01";

const { DUNE_API_KEY } = process.env;

const encoding = "utf-8";
const queryId = 3680107;

const INPUT_FOLDER = "input/addresses.txt";
const OUTPUT_FOLDER = "output/result.csv";

const createFile = (filePath: string) => {
  const dir = path.dirname(filePath);

  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(filePath, "");
  }
};

const main = async () => {
  createFile(INPUT_FOLDER);
  createFile(OUTPUT_FOLDER);

  if (!DUNE_API_KEY) throw new Error(`DUNE_API_KEY is not defined`);

  const addresses = fs
    .readFileSync("input/addresses.txt", { encoding })
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  console.log(`found ${addresses.length} addresses`);

  if (!addresses.length) return;

  const addrList = `(${addresses.join(",")})`;

  const parameters = [
    QueryParameter.text("addr_list", addrList),
    QueryParameter.text("start_date", `'${START_DATE}'`),
  ];

  const { result } = await new DuneClient(DUNE_API_KEY).refresh(
    queryId,
    parameters,
  );

  const rows = result?.rows;

  if (!rows) throw new Error("rows is not defined");

  const data = addresses.map((address) => {
    const lowerAddress = address.toLowerCase();

    const row = rows.find((r) => r.address === lowerAddress);

    if (!row) return "";

    const {
      txs,
      contracts,
      days,
      weeks,
      months,
      volume_usd,
      initial_time,
      last_time,
      age_days,
    } = row;

    return [
      address,
      txs,
      contracts,
      days,
      weeks,
      months,
      volume_usd,
      initial_time,
      last_time,
      age_days,
    ].join(",");
  });

  const header = [
    "address",
    "txs",
    "contracts",
    "days",
    "weeks",
    "months",
    "eth_volume_usd",
    "initial_time",
    "last_time",
    "age_days",
  ];

  fs.writeFileSync("output/result.csv", [header, ...data].join("\n"), {
    encoding,
  });
};

main();
