import * as parquet_types from './types';
import { ParquetSchema } from './schema';
import { Page, PageData, ParquetField, ParquetType } from './declare';

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm..
 *
 * The buffer argument must point to an object into which the shredded record
 * will be returned. You may re-use the buffer for repeated calls to this function
 * to append to an existing buffer, as long as the schema is unchanged.
 *
 * The format in which the shredded records will be stored in the buffer is as
 * follows:
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 *
 */

export interface RecordBuffer {
  columnData?: Record<string, PageData>;
  rowCount?: number;
  pageRowCount?: number;
  pages?: Record<string, Page[]>;
}

export const shredRecord = function (schema: ParquetSchema, record: Record<string, unknown>, buffer: RecordBuffer) {
  /* initialize buffer if needed */
  if (!('columnData' in buffer) || !('rowCount' in buffer)) {
    buffer.rowCount = 0;
    buffer.pageRowCount = 0;
    buffer.columnData = {};
    buffer.pages = {};

    for (const field of schema.fieldList) {
      const path = field.path.join(',');
      buffer.columnData[path] = {
        dlevels: [],
        rlevels: [],
        values: [],
        distinct_values: new Set(),
        count: 0,
      };
      buffer.pages[path] = [];
    }
  }

  /* shred the record directly into the buffer */
  shredRecordInternal(schema.fields, record, buffer.columnData!, 0, 0);

  (buffer.rowCount as number) += 1;
  (buffer.pageRowCount as number) += 1;
};

function shredRecordInternal(
  fields: Record<string, ParquetField>,
  record: Record<string, unknown> | null,
  data: Record<string, PageData>,
  rlvl: number,
  dlvl: number
) {
  for (const fieldName in fields) {
    const field = fields[fieldName];
    const fieldType = field.originalType || field.primitiveType;
    const path = field.path.join(',');
    const column = data[path];

    // Cache toPrimitive function to avoid repeated lookups
    let toPrimitiveFunc: ((value: unknown) => unknown) | null = null;
    if (!field.isNested) {
      // Get the toPrimitive function once per field
      const typeData = parquet_types.getParquetTypeDataObject(fieldType as ParquetType, field);
      toPrimitiveFunc = typeData.toPrimitive;
    }

    // fetch values
    let values: unknown[] = [];
    if (record && fieldName in record && record[fieldName] !== undefined && record[fieldName] !== null) {
      if (Array.isArray(record[fieldName])) {
        values = record[fieldName] as unknown[];
      } else if (ArrayBuffer.isView(record[fieldName])) {
        // checks if any typed array
        if (record[fieldName] instanceof Uint8Array) {
          // wrap in a buffer, since not supported by parquet_thrift
          values.push(Buffer.from(record[fieldName]));
        } else {
          throw new Error(Object.prototype.toString.call(record[fieldName]) + ' is not supported');
        }
      } else {
        values.push(record[fieldName]);
      }
    }

    // check values
    if (values.length == 0 && !!record && field.repetitionType === 'REQUIRED') {
      throw new Error('missing required field: ' + field.name);
    }

    if (values.length > 1 && field.repetitionType !== 'REPEATED') {
      throw new Error('too many values for field: ' + field.name);
    }

    // push null
    if (values.length == 0) {
      if (field.isNested && isDefined(field.fields)) {
        shredRecordInternal(field.fields, null, data, rlvl, dlvl);
      } else {
        column.rlevels!.push(rlvl);
        column.dlevels!.push(dlvl);
        column.count! += 1;
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; ++i) {
      const rlvl_i = i === 0 ? rlvl : field.rLevelMax;

      if (field.isNested && isDefined(field.fields)) {
        shredRecordInternal(field.fields, values[i] as Record<string, unknown>, data, rlvl_i, field.dLevelMax);
      } else {
        column.distinct_values!.add(values[i]);
        // Use cached toPrimitive function instead of dispatcher
        column.values!.push(toPrimitiveFunc!(values[i]) as any);
        column.rlevels!.push(rlvl_i as number);
        column.dlevels!.push(field.dLevelMax);
        column.count! += 1;
      }
    }
  }
}

/**
 * 'Materialize' a list of <value, repetition_level, definition_level>
 * tuples back to nested records (objects/arrays) using the Google Dremel
 * Algorithm..
 *
 * The buffer argument must point to an object with the following structure (i.e.
 * the same structure that is returned by shredRecords):
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 *
 */

export const materializeRecords = function (
  schema: ParquetSchema,
  buffer: RecordBuffer,
  records?: Record<string, unknown>[]
) {
  if (!records) {
    records = [];
  }

  for (const k in buffer.columnData) {
    const field = schema.findField(k);
    const fieldBranch = schema.findFieldBranch(k);
    const values = buffer.columnData[k].values![Symbol.iterator]();

    const rLevels = new Array(field.rLevelMax + 1);
    rLevels.fill(0);

    for (let i = 0; i < buffer.columnData[k].count!; ++i) {
      const dLevel = buffer.columnData[k].dlevels![i];
      const rLevel = buffer.columnData[k].rlevels![i];

      rLevels[rLevel]++;
      rLevels.fill(0, rLevel + 1);

      let value = null;
      if (dLevel === field.dLevelMax) {
        value = parquet_types.fromPrimitive(field.originalType || field.primitiveType, values.next().value, field);
      }

      records[rLevels[0] - 1] = records[rLevels[0] - 1] || {};

      materializeRecordField(
        records[rLevels[0] - 1] as Record<string, unknown>,
        fieldBranch,
        rLevels.slice(1),
        dLevel,
        value
      );
    }
  }

  return records;
};

function materializeRecordField(
  record: Record<string, unknown>,
  branch: ParquetField[],
  rLevels: number[],
  dLevel: number,
  value: Record<string, unknown>
) {
  const node = branch[0];

  if (dLevel < node.dLevelMax) {
    // This ensures that nulls are correctly processed
    record[node.name] = value;
    return;
  }

  if (branch.length > 1) {
    if (node.repetitionType === 'REPEATED') {
      if (!(node.name in record)) {
        record[node.name] = [];
      }
      const recordValue = record[node.name] as Record<string, unknown>[];

      while (recordValue.length < rLevels[0] + 1) {
        recordValue.push({});
      }

      materializeRecordField(recordValue[rLevels[0]], branch.slice(1), rLevels.slice(1), dLevel, value);
    } else {
      record[node.name] = record[node.name] || {};

      const recordValue = record[node.name] as Record<string, unknown>;
      materializeRecordField(recordValue, branch.slice(1), rLevels, dLevel, value);
    }
  } else {
    if (node.repetitionType === 'REPEATED') {
      if (!(node.name in record)) {
        record[node.name] = [];
      }
      const recordValue = record[node.name] as (Record<string, unknown> | null)[];

      while (recordValue.length < rLevels[0] + 1) {
        recordValue.push(null);
      }

      recordValue[rLevels[0]] = value;
    } else {
      record[node.name] = value;
    }
  }
}

function isDefined<T>(val: T | undefined): val is T {
  return val !== undefined;
}
