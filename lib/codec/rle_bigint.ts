import varint from 'varint';

function encodeRunBitpacked(values: bigint[], opts: { bitWidth: number }) {
  for (let i = 0; i < values.length % 8; i++) {
    values.push(0n);
  }

  const buf = Buffer.alloc(Math.ceil(opts.bitWidth * (values.length / 8)));
  for (let b = 0; b < opts.bitWidth * values.length; ++b) {
    if ((values[Math.floor(b / opts.bitWidth)] & (1n << BigInt(b % opts.bitWidth))) > 0n) {
      buf[Math.floor(b / 8)] |= 1 << b % 8;
    }
  }

  return Buffer.concat([Buffer.from(varint.encode(((values.length / 8) << 1) | 1)), buf]);
}

function encodeRunRepeated(value: bigint, count: number, opts: { bitWidth: number }) {
  const buf = Buffer.alloc(Math.ceil(opts.bitWidth / 8));
  let remainingValue = value;

  for (let i = 0; i < buf.length; ++i) {
    buf.writeUInt8(Number(remainingValue & 0xffn), i);
    remainingValue = remainingValue >> 8n;
  }

  return Buffer.concat([Buffer.from(varint.encode(count << 1)), buf]);
}

function unknownToParsedBigInt(value: string | number | bigint): bigint {
  if (typeof value === 'string') {
    return BigInt(value);
  } else if (typeof value === 'number') {
    return BigInt(value);
  } else {
    return value;
  }
}

export const encodeValuesBigInt = function (
  type: string,
  values: (number | bigint)[],
  opts: { bitWidth: number; disableEnvelope?: boolean }
) {
  if (!('bitWidth' in opts)) {
    throw new Error('bitWidth is required');
  }

  let bigintValues: bigint[];
  switch (type) {
    case 'BOOLEAN':
    case 'INT32':
    case 'INT64':
      bigintValues = values.map((x) => unknownToParsedBigInt(x));
      break;

    default:
      throw new Error('unsupported type: ' + type);
  }

  let buf = Buffer.alloc(0);
  let run: bigint[] = [];
  let repeats = 0;

  for (let i = 0; i < bigintValues.length; i++) {
    if (repeats === 0 && run.length % 8 === 0 && bigintValues[i] === bigintValues[i + 1]) {
      if (run.length) {
        buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)]);
        run = [];
      }
      repeats = 1;
    } else if (repeats > 0 && bigintValues[i] === bigintValues[i - 1]) {
      repeats += 1;
    } else {
      if (repeats) {
        buf = Buffer.concat([buf, encodeRunRepeated(bigintValues[i - 1], repeats, opts)]);
        repeats = 0;
      }
      run.push(bigintValues[i]);
    }
  }

  if (repeats) {
    buf = Buffer.concat([buf, encodeRunRepeated(bigintValues[bigintValues.length - 1], repeats, opts)]);
  } else if (run.length) {
    buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)]);
  }

  if (opts.disableEnvelope) {
    return buf;
  }

  const envelope = Buffer.alloc(buf.length + 4);
  envelope.writeUInt32LE(buf.length);
  buf.copy(envelope, 4);

  return envelope;
};
