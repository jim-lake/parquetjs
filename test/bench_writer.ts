import { PassThrough } from 'stream';
import * as parquet from '../parquet';

// BENCHMARK CONFIGURATION
const ROWS_PER_BATCH = 10_000;
const RUNS = 3;
const DURATION = 5;
const ROW_GROUP_SIZE = 4096;
const PAGE_SIZE = 8192;

// RANDOM DATA GENERATION CONSTANTS
const COL1_LENGTH = 10;
const TIMESTAMP_RANGE_SECONDS = 60;
const UNIQUE_PERCENTAGE = 0.15; // 15% unique values
const COL2_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const COL3_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const COL4_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const COL5_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const NUM1_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const NUM2_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const NUM3_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);
const NUM4_UNIQUE_COUNT = Math.floor(ROWS_PER_BATCH * UNIQUE_PERCENTAGE);

let totalBytesWritten = 0;

const schema = new parquet.ParquetSchema({
  col1: { type: 'UTF8' },
  timestamp1: { type: 'TIMESTAMP_MICROS' },
  timestamp2: { type: 'TIMESTAMP_MICROS' },
  col2: { type: 'UTF8' },
  col3: { type: 'UTF8' },
  col4: { type: 'UTF8' },
  col5: { type: 'UTF8' },
  num1: { type: 'DOUBLE' },
  num2: { type: 'DOUBLE' },
  num3: { type: 'DOUBLE' },
  num4: { type: 'DOUBLE' },
});

// PRE-GENERATED TEMPLATE DATA
const templates: Array<{
  col1: string;
  timestamp1: bigint;
  timestamp2: bigint;
  col2: string;
  col3: string;
  col4: string;
  col5: string;
  num1: number;
  num2: number;
  num3: number;
  num4: number;
}> = [];

function generateRandomString(length: number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let result = '';
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

function generateUniqueValues<T>(count: number, generator: () => T): T[] {
  const values: T[] = [];
  for (let i = 0; i < count; i++) {
    values.push(generator());
  }
  return values;
}

function generateRandomTimestamp(): bigint {
  const baseTime = Date.now() * 1000;
  const randomOffset = Math.floor(Math.random() * TIMESTAMP_RANGE_SECONDS * 1000) * 1000;
  return BigInt(baseTime + randomOffset);
}

// Generate unique value pools
const col2Values = generateUniqueValues(COL2_UNIQUE_COUNT, () => generateRandomString(5));
const col3Values = generateUniqueValues(COL3_UNIQUE_COUNT, () => generateRandomString(8));
const col4Values = generateUniqueValues(COL4_UNIQUE_COUNT, () => generateRandomString(6));
const col5Values = generateUniqueValues(COL5_UNIQUE_COUNT, () => generateRandomString(7));
const num1Values = generateUniqueValues(NUM1_UNIQUE_COUNT, () => Math.random() * 1000);
const num2Values = generateUniqueValues(NUM2_UNIQUE_COUNT, () => Math.random() * 100);
const num3Values = generateUniqueValues(NUM3_UNIQUE_COUNT, () => Math.random() * 10);
const num4Values = generateUniqueValues(NUM4_UNIQUE_COUNT, () => Math.random());

// Pre-generate all templates
for (let i = 0; i < ROWS_PER_BATCH; i++) {
  const timestamp1 = generateRandomTimestamp();
  const timestamp2 = generateRandomTimestamp();

  templates.push({
    col1: generateRandomString(COL1_LENGTH),
    timestamp1: BigInt(Math.floor(Number(timestamp1) / 1000) * 1000),
    timestamp2: BigInt(Math.floor(Number(timestamp2) / 1000) * 1000),
    col2: col2Values[Math.floor(Math.random() * col2Values.length)],
    col3: col3Values[Math.floor(Math.random() * col3Values.length)],
    col4: col4Values[Math.floor(Math.random() * col4Values.length)],
    col5: col5Values[Math.floor(Math.random() * col5Values.length)],
    num1: num1Values[Math.floor(Math.random() * num1Values.length)],
    num2: num2Values[Math.floor(Math.random() * num2Values.length)],
    num3: num3Values[Math.floor(Math.random() * num3Values.length)],
    num4: num4Values[Math.floor(Math.random() * num4Values.length)],
  });
}

async function writeBatch(): Promise<void> {
  const stream = new PassThrough();

  // Track bytes written
  stream.on('data', (chunk: Buffer) => {
    totalBytesWritten += chunk.length;
  });

  // Discard data
  stream.on('data', () => {});

  const writer = await parquet.ParquetWriter.openStream(schema, stream);
  writer.setRowGroupSize(ROW_GROUP_SIZE);
  writer.setPageSize(PAGE_SIZE);

  for (let i = 0; i < ROWS_PER_BATCH; i++) {
    await writer.appendRow(templates[i]);
  }

  await writer.close();
}

async function main() {
  console.log('Warming up...');
  await writeBatch();
  console.log('Starting benchmark...');

  const stats = [];
  for (let run = 1; run <= RUNS; run++) {
    let rows = 0;
    const startBytes = totalBytesWritten;
    console.log('start run:', run);
    const startTime = process.hrtime.bigint();
    const targetEndTime = startTime + BigInt(DURATION * 1e9);

    while (process.hrtime.bigint() < targetEndTime) {
      await writeBatch();
      rows += ROWS_PER_BATCH;
    }

    const actualEndTime = process.hrtime.bigint();
    const actualDuration = Number(actualEndTime - startTime) / 1e9;
    const runBytes = totalBytesWritten - startBytes;
    const rowsPerSecond = Intl.NumberFormat().format(Math.round(rows / actualDuration));
    const bytesPerSecond = Intl.NumberFormat().format(Math.round(runBytes / actualDuration));
    stats.push(
      `Run ${run}: ${rowsPerSecond} rows/second, ${bytesPerSecond} bytes/second (${Intl.NumberFormat().format(rows)} rows, ${Intl.NumberFormat().format(runBytes)} bytes in ${actualDuration.toFixed(3)}s)`
    );
  }

  console.log('');
  stats.forEach((s) => console.log(s));
  console.log('');
  console.log(`Total bytes written: ${Intl.NumberFormat().format(totalBytesWritten)}`);
}

main().catch(console.error);
