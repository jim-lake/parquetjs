'use strict';
const chai = require('chai');
const os = require('os');
const path = require('path');
const assert = chai.assert;
const parquet = require('../parquet');

/*
  This test creates a test file that has an annotated LIST wrapper that works with AWS Athena
  Currently the schema (and the input data) needs to follow the specification for an annotated list

  The Athena schema for this test is `id string, test (array<struct<a:string,b:string>>)`
  but instead of input data `{id: 'Row1', test: [{a: 'test1', b: 1}, {a: 'test2', b: 2}, {a: 'test3', b: 3}]}`
  we need to wrap the data inside `list` and every element inside `element`, i.e:
  `{id: 'Row1', test: {list: [{element: {a:'test1', b:1}}, {element: { a: 'test2', b: 2}}, {element: {a: 'test3', b: 3}}]}`
  and the schema needs to match this structure as well (see listSchema below)

  To see a working example on Athena, run this test and copy the list.parquet file to an s3 bucket.

  In Athena create the listTest table with the following command:

  CREATE EXTERNAL TABLE `listTest`(
    id string,
    `test` array<struct<a:string,b:int>>
  )
  ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION
    's3://s3bucket/.../list.parquet'

  And verify that Athena parses the parquet file correctly by `SELECT * from listTest`
*/

const listStructSchema = new parquet.ParquetSchema({
  id: { type: 'UTF8' },
  test: {
    type: 'LIST',
    fields: {
      list: {
        repeated: true,
        fields: {
          element: {
            fields: {
              a: { type: 'UTF8' },
              b: { type: 'INT64' },
            },
          },
        },
      },
    },
  },
});

const listArraySchema = new parquet.ParquetSchema({
  id: { type: 'UTF8' },
  test: {
    type: 'LIST',
    fields: {
      list: {
        repeated: true,
        fields: {
          element: {
            type: 'UTF8',
          },
        },
      },
    },
  },
});

describe('struct list', function () {
  let reader;

  const row1 = {
    id: 'Row1',
    test: {
      list: [
        { element: { a: 'test1', b: 1n } },
        { element: { a: 'test2', b: 2n } },
        { element: { a: 'test3', b: 3n } },
      ],
    },
  };

  const row2 = {
    id: 'Row2',
    test: { list: [{ element: { a: 'test4', b: 4n } }] },
  };

  before(async function () {
    let writer = await parquet.ParquetWriter.openFile(listStructSchema, path.join(os.tmpdir(), 'list.parquet'), {
      pageSize: 100,
    });

    writer.appendRow(row1);
    writer.appendRow(row2);

    await writer.close();
    reader = await parquet.ParquetReader.openFile(path.join(os.tmpdir(), 'list.parquet'));
  });

  it('schema is encoded correctly', async function () {
    const schema = reader.metadata.schema;
    assert.equal(schema.length, 7);
    assert.equal(schema[2].name, 'test');
    assert.equal(schema[2].converted_type, 3);
  });

  it('output matches input', async function () {
    const cursor = reader.getCursor();
    let row = await cursor.next();
    assert.deepEqual(row, row1);
    row = await cursor.next();
    assert.deepEqual(row, row2);
  });
});

describe('array list', function () {
  let reader;

  const row1 = {
    id: 'Row1',
    test: { list: [{ element: 'abcdef' }, { element: 'fedcba' }] },
  };

  const row2 = {
    id: 'Row2',
    test: { list: [{ element: 'ghijkl' }, { element: 'lkjihg' }] },
  };

  before(async function () {
    let writer = await parquet.ParquetWriter.openFile(listArraySchema, path.join(os.tmpdir(), 'list-array.parquet'), {
      pageSize: 100,
    });

    writer.appendRow(row1);
    writer.appendRow(row2);

    await writer.close();
    reader = await parquet.ParquetReader.openFile(path.join(os.tmpdir(), 'list-array.parquet'));
  });

  it('schema is encoded correctly', async function () {
    const schema = reader.metadata.schema;
    assert.equal(schema.length, 5);
    assert.equal(schema[2].name, 'test');
    assert.equal(schema[2].converted_type, 3);
  });

  it('output matches input', async function () {
    const cursor = reader.getCursor();
    let row = await cursor.next();
    assert.deepEqual(row, row1);
    row = await cursor.next();
    assert.deepEqual(row, row2);
  });
});
