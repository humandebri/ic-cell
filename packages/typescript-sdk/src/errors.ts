export class IcCellError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "IcCellError";
    this.code = code;
  }
}

export class CasConflictError extends IcCellError {
  readonly key: string;
  readonly expectedGeneration: bigint;
  readonly actualGeneration: bigint | null;

  constructor(key: string, expectedGeneration: bigint, actualGeneration: bigint | null) {
    super(
      "CAS_CONFLICT",
      `CAS conflict on '${key}': expected generation ${expectedGeneration}, actual ${actualGeneration}`,
    );
    this.name = "CasConflictError";
    this.key = key;
    this.expectedGeneration = expectedGeneration;
    this.actualGeneration = actualGeneration;
  }
}

export class StaleEpochError extends IcCellError {
  readonly cellId: string;
  readonly expectedEpoch: bigint;
  readonly actualEpoch: bigint;

  constructor(cellId: string, expectedEpoch: bigint, actualEpoch: bigint) {
    super(
      "STALE_EPOCH",
      `Stale epoch for cell '${cellId}': expected ${expectedEpoch}, actual ${actualEpoch}`,
    );
    this.name = "StaleEpochError";
    this.cellId = cellId;
    this.expectedEpoch = expectedEpoch;
    this.actualEpoch = actualEpoch;
  }
}

export class LeaseHeldError extends IcCellError {
  readonly cellId: string;
  readonly currentOwner: string;
  readonly expiresAtNs: bigint;

  constructor(cellId: string, currentOwner: string, expiresAtNs: bigint) {
    super("LEASE_HELD", `Lease for '${cellId}' is held by '${currentOwner}' until ${expiresAtNs}`);
    this.name = "LeaseHeldError";
    this.cellId = cellId;
    this.currentOwner = currentOwner;
    this.expiresAtNs = expiresAtNs;
  }
}

export class PreconditionFailedError extends IcCellError {
  constructor(reason: string) {
    super("PRECONDITION_FAILED", reason);
    this.name = "PreconditionFailedError";
  }
}

export class UnknownOutcomeError extends IcCellError {
  readonly idempotencyKey?: string;
  constructor(message: string, idempotencyKey?: string) {
    super("UNKNOWN_OUTCOME", message);
    this.name = "UnknownOutcomeError";
    this.idempotencyKey = idempotencyKey;
  }
}

/** The cell is owned by a node that could not be reached; retry later. */
export class CellUnavailableError extends IcCellError {
  readonly cellId: string;
  constructor(cellId: string, message: string) {
    super("CELL_UNAVAILABLE", message);
    this.name = "CellUnavailableError";
    this.cellId = cellId;
  }
}

/** True for errors where retrying the same idempotent request is safe. */
export function isTransientError(err: unknown): boolean {
  return err instanceof IcCellError
    ? err.code === "TRANSPORT" || err.code === "UNKNOWN_OUTCOME"
    : !(err instanceof TypeError || err instanceof RangeError);
}
