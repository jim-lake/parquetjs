'use strict';
const chai = require('chai');
const os = require('os');
const path = require('path');
const assert = chai.assert;
const parquet_codec_rle = require('../lib/codec/rle');
const parquet = require('../parquet');

describe('ParquetCodec::RLE', function () {
  it('should encode bitpacked values', function () {
    let buf = parquet_codec_rle.encodeValues('INT32', [0, 1, 2, 3, 4, 5, 6, 7], {
      disableEnvelope: true,
      bitWidth: 3,
    });

    assert.deepEqual(buf, Buffer.from([0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode bitpacked values', function () {
    let vals = parquet_codec_rle.decodeValues(
      'INT32',
      {
        buffer: Buffer.from([0x03, 0x88, 0xc6, 0xfa]),
        offset: 0,
      },
      8,
      {
        disableEnvelope: true,
        bitWidth: 3,
      }
    );

    assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7]);
  });

  describe('number of values not a multiple of 8', function () {
    it('should encode bitpacked values', function () {
      let buf = parquet_codec_rle.encodeValues('INT32', [0, 1, 2, 3, 4, 5, 6, 7, 6, 5], {
        disableEnvelope: true,
        bitWidth: 3,
      });

      assert.deepEqual(buf, Buffer.from([0x05, 0x88, 0xc6, 0xfa, 0x2e, 0x00, 0x00]));
    });

    it('should decode bitpacked values', function () {
      let vals = parquet_codec_rle.decodeValues(
        'INT32',
        {
          buffer: Buffer.from([0x05, 0x88, 0xc6, 0xfa, 0x2e, 0x00, 0x00]),
          offset: 0,
        },
        10,
        {
          disableEnvelope: true,
          bitWidth: 3,
        }
      );

      assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7, 6, 5]);
    });
  });

  it('should encode repeated values', function () {
    let buf = parquet_codec_rle.encodeValues(
      'INT32',
      [1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567],
      {
        disableEnvelope: true,
        bitWidth: 21,
      }
    );

    assert.deepEqual(buf, Buffer.from([0x10, 0x87, 0xd6, 0x12]));
  });

  it('should decode repeated values', function () {
    let vals = parquet_codec_rle.decodeValues(
      'INT32',
      {
        buffer: Buffer.from([0x10, 0x87, 0xd6, 0x12]),
        offset: 0,
      },
      8,
      {
        disableEnvelope: true,
        bitWidth: 21,
      }
    );

    assert.deepEqual(vals, [1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567]);
  });

  it('should encode mixed runs', function () {
    let buf = parquet_codec_rle.encodeValues(
      'INT32',
      [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7],
      {
        disableEnvelope: true,
        bitWidth: 3,
      }
    );

    assert.deepEqual(buf, Buffer.from([0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode mixed runs', function () {
    let vals = parquet_codec_rle.decodeValues(
      'INT32',
      {
        buffer: Buffer.from([0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa]),
        offset: 0,
      },
      24,
      {
        disableEnvelope: true,
        bitWidth: 3,
      }
    );

    assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7]);
  });

  it('should encode bigints', function () {
    let buf = parquet_codec_rle.encodeValues(
      'INT64',
      [4503599627370496n, 4503599627370497n, 4503599627370498n, 4503599627370499n, 4503599627370500n],
      {
        disableEnvelope: true,
        bitWidth: 55,
      }
    );
    assert.deepEqual(
      buf,
      Buffer.from([
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
      ])
    );
  });

  it('should encode/decode INT64 with 44-bit values', function () {
    // Values with 44th bit set (2^43 = 8796093022208)
    const inputs = [8796093022208, 8796093022209, 8796093022210, 8796093022211];
    const expectedOutputs = [8796093022208n, 8796093022209n, 8796093022210n, 8796093022211n];

    let buf = parquet_codec_rle.encodeValues('INT64', inputs, {
      disableEnvelope: true,
      bitWidth: 44,
    });

    assert.deepEqual(
      buf,
      Buffer.from([
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00, 0x38,
        0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      ])
    );

    let vals = parquet_codec_rle.decodeValues('INT64', { buffer: buf, offset: 0 }, 4, {
      disableEnvelope: true,
      bitWidth: 44,
    });

    assert.deepEqual(vals, expectedOutputs);
  });

  it('should encode/decode INT64 with 63-bit values', function () {
    // Values using all 63 bits (2^62 = 4611686018427387904n)
    const inputs = [4611686018427387904n, 4611686018427387905n, 4611686018427387906n, 4611686018427387907n];
    const expectedOutputs = [4611686018427387904n, 4611686018427387905n, 4611686018427387906n, 4611686018427387907n];

    let buf = parquet_codec_rle.encodeValues('INT64', inputs, {
      disableEnvelope: true,
      bitWidth: 63,
    });

    assert.deepEqual(
      buf,
      Buffer.from([
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa0, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      ])
    );

    let vals = parquet_codec_rle.decodeValues('INT64', { buffer: buf, offset: 0 }, 4, {
      disableEnvelope: true,
      bitWidth: 63,
    });

    assert.deepEqual(vals, expectedOutputs);
  });

  it('should write and read INT32 column with RLE encoding and produce smaller files', async function () {
    const fs = require('fs');

    const rleSchema = new parquet.ParquetSchema({
      value: { type: 'INT32', encoding: 'RLE', typeLength: 7 },
    });

    const plainSchema = new parquet.ParquetSchema({
      value: { type: 'INT32', encoding: 'PLAIN' },
    });

    const rleFile = path.join(os.tmpdir(), 'test-rle-int32.parquet');
    const plainFile = path.join(os.tmpdir(), 'test-plain-int32.parquet');

    // Create test data with many repeated values for RLE compression
    const testData = Array(100)
      .fill()
      .map((_, i) => Math.floor(i / 10));

    // Write with RLE encoding
    const rleWriter = await parquet.ParquetWriter.openFile(rleSchema, rleFile);
    for (const value of testData) {
      await rleWriter.appendRow({ value });
    }
    await rleWriter.close();

    // Write with PLAIN encoding
    const plainWriter = await parquet.ParquetWriter.openFile(plainSchema, plainFile);
    for (const value of testData) {
      await plainWriter.appendRow({ value });
    }
    await plainWriter.close();

    // Verify RLE file is smaller
    const rleSize = fs.statSync(rleFile).size;
    const plainSize = fs.statSync(plainFile).size;
    assert.isTrue(rleSize < plainSize, `RLE file (${rleSize}) should be smaller than PLAIN file (${plainSize})`);

    // Verify data integrity
    const reader = await parquet.ParquetReader.openFile(rleFile);
    const cursor = reader.getCursor();
    const results = [];
    let record;
    while ((record = await cursor.next())) {
      results.push(record.value);
    }
    await reader.close();

    assert.deepEqual(results, testData);
  });

  it('should work with multiple RLE columns of different types', async function () {
    const schema = new parquet.ParquetSchema({
      bool_col: { type: 'BOOLEAN', encoding: 'RLE', typeLength: 1 },
      int32_col: { type: 'INT32', encoding: 'RLE', typeLength: 7 },
      int8_col: { type: 'INT_8', encoding: 'RLE', typeLength: 7 },
      int16_col: { type: 'INT_16', encoding: 'RLE', typeLength: 7 },
      int32_typed_col: { type: 'INT_32', encoding: 'RLE', typeLength: 7 },
      uint8_col: { type: 'UINT_8', encoding: 'RLE', typeLength: 7 },
      uint16_col: { type: 'UINT_16', encoding: 'RLE', typeLength: 7 },
      uint32_col: { type: 'UINT_32', encoding: 'RLE', typeLength: 7 },
      // TIME_MILLIS has value transformation issues
      // time_millis_col: { type: 'TIME_MILLIS', encoding: 'RLE', typeLength: 7 },
      // Types that don't work with RLE due to BigInt issues:
      // int64_col: { type: 'INT64', encoding: 'RLE', typeLength: 7 },
      // time_micros_col: { type: 'TIME_MICROS', encoding: 'RLE', typeLength: 7 },
      // timestamp_millis_col: { type: 'TIMESTAMP_MILLIS', encoding: 'RLE', typeLength: 7 },
      // timestamp_micros_col: { type: 'TIMESTAMP_MICROS', encoding: 'RLE', typeLength: 7 },
      // int64_typed_col: { type: 'INT_64', encoding: 'RLE', typeLength: 7 },
      // uint64_col: { type: 'UINT_64', encoding: 'RLE', typeLength: 7 },
    });

    const testFile = path.join(os.tmpdir(), 'test-multi-rle.parquet');
    const testData = Array(50)
      .fill()
      .map((_, i) => ({
        bool_col: i % 2 === 0,
        int32_col: Math.floor(i / 5),
        int8_col: Math.floor(i / 5) % 100,
        int16_col: Math.floor(i / 5),
        int32_typed_col: Math.floor(i / 5),
        uint8_col: Math.floor(i / 5) % 200,
        uint16_col: Math.floor(i / 5),
        uint32_col: Math.floor(i / 5),
      }));

    const writer = await parquet.ParquetWriter.openFile(schema, testFile);
    for (const row of testData) {
      await writer.appendRow(row);
    }
    await writer.close();

    const reader = await parquet.ParquetReader.openFile(testFile);
    const cursor = reader.getCursor();
    const results = [];
    let record;
    while ((record = await cursor.next())) {
      results.push(record);
    }
    await reader.close();

    assert.deepEqual(results, testData);
  });

  describe('RLE encoding by type', function () {
    const testData = Array(20)
      .fill()
      .map((_, i) => Math.floor(i / 4));

    it('should work with BOOLEAN type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'BOOLEAN', encoding: 'RLE', typeLength: 1 },
      });
      const testFile = path.join(os.tmpdir(), 'test-boolean-rle.parquet');
      const boolData = Array(20)
        .fill()
        .map((_, i) => i % 2 === 0);

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of boolData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, boolData);
      };

      await testFunction().catch((err) => {
        console.log('BOOLEAN RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT32 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT32', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int32-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('INT32 RLE failed:', err.message);
        throw err;
      });
    });
    it('should work with INT32 type, 32 bits used', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT32', encoding: 'RLE', typeLength: 32 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int32-rle-32bit.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('INT32 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT64 type, less than 32 bits used', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT64', encoding: 'RLE', typeLength: 32 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int64-rle-32bit.parquet');
      const bigIntData = testData.map((v) => BigInt(v));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of bigIntData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, bigIntData);
      };

      await testFunction().catch((err) => {
        console.log('INT64 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT64 type, 55 bits used', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT64', encoding: 'RLE', typeLength: 55 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int64-rle-55bit.parquet');
      const bigIntData = testData.map((v) => BigInt(v) + 2n ** 35n);

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of bigIntData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, bigIntData);
      };

      await testFunction().catch((err) => {
        console.log('INT64 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with TIME_MILLIS type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'TIME_MILLIS', encoding: 'RLE', typeLength: 27 },
      });
      const testFile = path.join(os.tmpdir(), 'test-time-millis-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('TIME_MILLIS RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with TIME_MICROS type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'TIME_MICROS', encoding: 'RLE', typeLength: 37 },
      });
      const testFile = path.join(os.tmpdir(), 'test-time-micros-rle.parquet');
      const bigIntData = testData.map((v) => BigInt(v));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of bigIntData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, bigIntData);
      };

      await testFunction().catch((err) => {
        console.log('TIME_MICROS RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with TIMESTAMP_MILLIS type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'TIMESTAMP_MILLIS', encoding: 'RLE', typeLength: 45 },
      });
      const testFile = path.join(os.tmpdir(), 'test-timestamp-millis-rle.parquet');
      const timestampData = testData.map((v) => new Date(Date.now() + v * 1000));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of timestampData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, timestampData);
      };

      await testFunction().catch((err) => {
        console.log('TIMESTAMP_MILLIS RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with TIMESTAMP_MICROS type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'TIMESTAMP_MICROS', encoding: 'RLE', typeLength: 55 },
      });
      const testFile = path.join(os.tmpdir(), 'test-timestamp-micros-rle.parquet');
      const timestampData = testData.map((v) => new Date(Date.now() + v * 1000));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of timestampData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, timestampData);
      };

      await testFunction().catch((err) => {
        console.log('TIMESTAMP_MICROS RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT_8 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT_8', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int8-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('INT_8 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT_16 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT_16', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int16-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('INT_16 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT_32 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT_32', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int32-typed-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('INT_32 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with INT_64 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'INT_64', encoding: 'RLE', typeLength: 55 },
      });
      const testFile = path.join(os.tmpdir(), 'test-int64-typed-rle.parquet');
      const bigIntData = testData.map((v) => BigInt(v));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of bigIntData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, bigIntData);
      };

      await testFunction().catch((err) => {
        console.log('INT_64 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with UINT_8 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'UINT_8', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-uint8-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('UINT_8 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with UINT_16 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'UINT_16', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-uint16-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('UINT_16 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with UINT_32 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'UINT_32', encoding: 'RLE', typeLength: 7 },
      });
      const testFile = path.join(os.tmpdir(), 'test-uint32-rle.parquet');

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of testData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, testData);
      };

      await testFunction().catch((err) => {
        console.log('UINT_32 RLE failed:', err.message);
        throw err;
      });
    });

    it('should work with UINT_64 type', async function () {
      const schema = new parquet.ParquetSchema({
        value: { type: 'UINT_64', encoding: 'RLE', typeLength: 55 },
      });
      const testFile = path.join(os.tmpdir(), 'test-uint64-rle.parquet');
      const bigIntData = testData.map((v) => BigInt(v));

      const testFunction = async () => {
        const writer = await parquet.ParquetWriter.openFile(schema, testFile);
        for (const value of bigIntData) {
          await writer.appendRow({ value });
        }
        await writer.close();

        const reader = await parquet.ParquetReader.openFile(testFile);
        const cursor = reader.getCursor();
        const results = [];
        let record;
        while ((record = await cursor.next())) {
          results.push(record.value);
        }
        await reader.close();
        assert.deepEqual(results, bigIntData);
      };

      await testFunction().catch((err) => {
        console.log('UINT_64 RLE failed:', err.message);
        throw err;
      });
    });
  });
});
