import { Cursor } from './types';
export declare const encodeValues: (type: string, values: (number | bigint)[], opts: {
    bitWidth: number;
    disableEnvelope?: boolean;
}) => Buffer<ArrayBuffer>;
export declare const decodeValues: (type: string, cursor: Cursor, count: number, opts: {
    bitWidth: number;
    disableEnvelope?: boolean;
}) => any[];
