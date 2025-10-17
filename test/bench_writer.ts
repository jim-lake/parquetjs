import { PassThrough } from 'stream';
import * as parquet from '../parquet';

const ROWS_PER_BATCH = 10_000;
const RUNS = 3;
const DURATION = 5;
const ROW_GROUP_SIZE = 4096;
const PAGE_SIZE = 8192;

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

const template = {
  col1: 'testapp',
  timestamp1: BigInt(Date.now() * 1000),
  timestamp2: BigInt(Date.now() * 1000),
  col2: '123',
  col3: 'dt123',
  col4: 'stat',
  col5: 'test',
  num1: 1.23,
  num2: 4.56,
  num3: 7.89,
  num4: 0.12,
};

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
    await writer.appendRow(template);
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
    const rowsPerSecond = Intl.NumberFormat().format(
      Math.round(rows / actualDuration)
    );
    const bytesPerSecond = Intl.NumberFormat().format(
      Math.round(runBytes / actualDuration)
    );
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
