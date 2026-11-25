/**
 * Value codec for cell storage. Values are stored as bytes in the canister;
 * the default codec is JSON extended with tags for types JSON cannot carry.
 */
export interface ValueCodec {
  encode(value: unknown): Uint8Array;
  decode(bytes: Uint8Array): unknown;
}

const TAG = "__ic$";

function toBase64(bytes: Uint8Array): string {
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

function tagged(type: string, value: unknown): Record<string, unknown> {
  return { [TAG]: type, v: value };
}

function toJsonValue(value: unknown): unknown {
  if (value === undefined) return tagged("undefined", 0);
  if (value === null || typeof value === "boolean" || typeof value === "string") return value;
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : tagged("number", String(value));
  }
  if (typeof value === "bigint") return tagged("bigint", value.toString());
  if (value instanceof Uint8Array) return tagged("bytes", toBase64(value));
  if (value instanceof Date) return tagged("date", value.toISOString());
  if (value instanceof Map) {
    return tagged(
      "map",
      Array.from(value.entries(), ([k, v]) => [toJsonValue(k), toJsonValue(v)]),
    );
  }
  if (value instanceof Set) return tagged("set", Array.from(value, toJsonValue));
  if (Array.isArray(value)) return value.map(toJsonValue);
  if (typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (v !== undefined) out[k] = toJsonValue(v);
    }
    if (TAG in out) {
      // Escape user objects that happen to use the tag key.
      return tagged("object", out);
    }
    return out;
  }
  throw new TypeError(`cannot encode value of type ${typeof value}`);
}

function fromJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(fromJsonValue);
  if (value === null || typeof value !== "object") return value;
  const obj = value as Record<string, unknown>;
  if (typeof obj[TAG] === "string" && "v" in obj && Object.keys(obj).length === 2) {
    const v = obj.v;
    switch (obj[TAG]) {
      case "undefined":
        return undefined;
      case "number":
        return Number(v);
      case "bigint":
        return BigInt(v as string);
      case "bytes":
        return fromBase64(v as string);
      case "date":
        return new Date(v as string);
      case "map":
        return new Map(
          (v as [unknown, unknown][]).map(([k, x]) => [fromJsonValue(k), fromJsonValue(x)]),
        );
      case "set":
        return new Set((v as unknown[]).map(fromJsonValue));
      case "object":
        return Object.fromEntries(
          Object.entries(v as Record<string, unknown>).map(([k, x]) => [k, fromJsonValue(x)]),
        );
    }
  }
  return Object.fromEntries(Object.entries(obj).map(([k, x]) => [k, fromJsonValue(x)]));
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** JSON + tags for `undefined`, non-finite numbers, bigint, Uint8Array, Date, Map, Set. */
export const taggedJsonCodec: ValueCodec = {
  encode(value) {
    return encoder.encode(JSON.stringify(toJsonValue(value)));
  },
  decode(bytes) {
    return fromJsonValue(JSON.parse(decoder.decode(bytes)));
  },
};

/** Serializes RPC payloads (arguments / results) with the same tagging. */
export function encodeJson(value: unknown): string {
  return JSON.stringify(toJsonValue(value));
}

export function decodeJson(text: string): unknown {
  return fromJsonValue(JSON.parse(text));
}
