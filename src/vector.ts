// Vector search support: per-plane feature probes and index-aware waiters.
//
// Vector search spans two planes with independent implementation surfaces: a
// target can parse `VectorIndexes` on CreateTable without implementing
// SearchVectors, or vice versa. Each plane therefore gets its own probe, and
// the tests for each plane gate on the probe for what they actually exercise.
// A single shared probe would mis-score a target that implements one side
// only.
//
// The waiters follow the shape of waitUntilActive / waitForGsiConsistency in
// helpers.ts: a ceiling expiring is an IndeterminateError (a failed
// observation), never a divergence.

import {
  CreateTableCommand,
  SearchVectorsCommand,
  DescribeTableCommand,
  DynamoDBServiceException,
  type AttributeValue,
  type VectorIndexDescription,
} from '@aws-sdk/client-dynamodb'
import { ddb } from './client.js'
import { region } from './aws-config.js'
import { uniqueTableName, waitUntilActive, deleteTable } from './helpers.js'
import { IndeterminateError } from './indeterminate.js'
import { isUnsupportedFault } from './unsupported.js'
import { supportsControlPlaneOp } from './infra.js'
import { ceilingsFor } from './regions.js'

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// ── Data-plane probe: SearchVectors ─────────────────────────────────────────

/**
 * Probe input for SearchVectors: a table that cannot exist (the reserved
 * `_conformance_` prefix plus a name no test creates). Real AWS answers
 * ResourceNotFoundException — a real error, so the operation counts as
 * implemented. A target without the operation answers an unsupported fault.
 */
const SEARCH_PROBE_INPUT = {
  TableName: '_conformance_no_such_table_probe',
  IndexName: 'no-such-index',
  SearchVector: [{ N: '1' }, { N: '0' }, { N: '0' }] as AttributeValue[],
  TopK: 1,
}

let searchVectorsSupport: boolean | undefined

/** Whether the target implements the SearchVectors operation. Memoised. */
export async function supportsSearchVectors(): Promise<boolean> {
  if (searchVectorsSupport === undefined) {
    searchVectorsSupport = await supportsControlPlaneOp(() =>
      ddb.send(new SearchVectorsCommand(SEARCH_PROBE_INPUT)),
    )
  }
  return searchVectorsSupport
}

/**
 * Feature-probe skip for describe blocks exercising the data plane of vector
 * search (SearchVectors itself, writes judged through an index, capacity
 * shapes).
 */
export function skipUnlessSearchVectors(): void {
  let supported = true
  beforeAll(async () => {
    supported = await supportsSearchVectors()
  })
  beforeEach(({ skip }) => {
    if (!supported) skip()
  })
}

// ── Control-plane probe: CreateTable with VectorIndexes ─────────────────────

let vectorIndexesSupport: boolean | undefined

/**
 * Whether the target implements vector indexes on the control plane. Memoised
 * across the run because the probe provisions a real table.
 *
 * Support means CreateTable accepts `VectorIndexes` AND DescribeTable reflects
 * the index back. The reflection check is what separates "implemented" from
 * "parsed and discarded": a target that accepts the parameter but drops it has
 * not implemented the surface, and counting that as implemented would convert
 * absent support into divergence on every lifecycle assertion. Recording it as
 * scope (skip) instead is deliberate lenience, mirroring how an unsupported
 * fault is treated.
 */
export async function supportsVectorIndexes(): Promise<boolean> {
  if (vectorIndexesSupport !== undefined) return vectorIndexesSupport
  const name = uniqueTableName('vector_probe')
  try {
    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        VectorIndexes: [
          {
            IndexName: 'probe-index',
            VectorAttribute: { AttributeName: 'embedding' },
            Dimensions: 3,
            DistanceFunction: 'COSINE',
            Projection: { ProjectionType: 'KEYS_ONLY' },
          },
        ],
      }),
    )
    const described = await ddb.send(new DescribeTableCommand({ TableName: name }))
    vectorIndexesSupport =
      (described.Table?.VectorIndexes ?? []).some((ix) => ix.IndexName === 'probe-index')
  } catch (e) {
    if (isUnsupportedFault(e)) {
      vectorIndexesSupport = false
    } else if (e instanceof DynamoDBServiceException && e.name === 'ValidationException') {
      // The probe's arguments are verified valid against real AWS, so a
      // ValidationException here is a target rejecting a parameter it does
      // not model. Lenience wins: absent support is scope, not divergence.
      vectorIndexesSupport = false
    } else {
      // Anything else (throttle, transport, access) is not an answer about
      // support. Err on "supported" so a transient hiccup cannot silently
      // skip the family; the tests' own classification handles the rest.
      vectorIndexesSupport = true
    }
  } finally {
    // The probe table may still be CREATING, where DeleteTable answers
    // ResourceInUseException and deleteTable's swallow would quietly leave
    // the table behind until the next run's sweep. Wait for ACTIVE first;
    // both steps are best-effort — cleanup must never fail the probe.
    await waitUntilActive(name).catch(() => {})
    await deleteTable(name).catch(() => {})
  }
  return vectorIndexesSupport
}

/**
 * Feature-probe skip for describe blocks exercising the control plane of
 * vector search (CreateTable/UpdateTable index lifecycle, DescribeTable
 * output, create-time validation).
 */
export function skipUnlessVectorIndexes(): void {
  let supported = true
  beforeAll(async () => {
    supported = await supportsVectorIndexes()
  })
  beforeEach(({ skip }) => {
    if (!supported) skip()
  })
}

// ── Combined gate ───────────────────────────────────────────────────────────

/**
 * Whether the target implements both planes. Files whose data-plane tests
 * must first PROVISION a vector-indexed table depend on both: a target with
 * SearchVectors but no CreateTable-with-VectorIndexes would otherwise fail
 * table creation in beforeAll — divergence — when the honest answer is scope.
 */
export async function supportsVectorSearch(): Promise<boolean> {
  return (await supportsSearchVectors()) && (await supportsVectorIndexes())
}

/**
 * Feature-probe skip for describe blocks that provision a vector-indexed
 * table and exercise it through the data plane.
 */
export function skipUnlessVectorSearch(): void {
  let supported = true
  beforeAll(async () => {
    supported = await supportsVectorSearch()
  })
  beforeEach(({ skip }) => {
    if (!supported) skip()
  })
}

// ── Waiters ─────────────────────────────────────────────────────────────────

/** Find one index's description on a table, or undefined. */
export async function describeVectorIndex(
  tableName: string,
  indexName: string,
): Promise<VectorIndexDescription | undefined> {
  const res = await ddb.send(new DescribeTableCommand({ TableName: tableName }))
  return (res.Table?.VectorIndexes ?? []).find((ix) => ix.IndexName === indexName)
}

/**
 * Wait until a vector index is ACTIVE and done backfilling.
 *
 * `Backfilling` is only reported for indexes added via UpdateTable; for an
 * index created with the table, DescribeTable leaves the field unset while
 * the status alone walks to ACTIVE (there is no BACKFILLING status value).
 * An unset field therefore counts as "not backfilling" on both paths.
 */
export async function waitForVectorIndexActive(
  tableName: string,
  indexName: string,
  opts: { timeoutMs?: number } = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? ceilingsFor(region).tableActiveMs
  const start = Date.now()
  let delay = 0
  while (Date.now() - start < timeoutMs) {
    const ix = await describeVectorIndex(tableName, indexName)
    if (ix?.IndexStatus === 'ACTIVE' && ix.Backfilling !== true) return
    if (delay > 0) await sleep(delay)
    // Grows: `delay || 500` re-evaluated to 500 on every pass, so this polled
    // at a flat 500ms and the 2000ms ceiling was unreachable.
    delay = delay === 0 ? 500 : Math.min(delay * 2, 2000)
  }
  throw new IndeterminateError(
    'vector-index-timeout',
    `Timeout waiting for vector index ${indexName} on ${tableName} to become ACTIVE`,
  )
}

/**
 * Wait until a search returns the expected number of results — the vector
 * index is eventually consistent with no documented visibility bound, so a
 * test must hold positive evidence that the index has caught up before it
 * asserts anything about search results (especially an absence).
 */
export async function waitForVectorSearchable(opts: {
  tableName: string
  indexName: string
  searchVector: AttributeValue[]
  expectedCount: number
  searchConditionExpression?: string
  expressionAttributeValues?: Record<string, AttributeValue>
  timeoutMs?: number
}): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? ceilingsFor(region).gsiConsistencyMs
  const start = Date.now()
  let delay = 0
  while (Date.now() - start < timeoutMs) {
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: opts.tableName,
        IndexName: opts.indexName,
        SearchVector: opts.searchVector,
        TopK: Math.max(opts.expectedCount, 1),
        SearchConditionExpression: opts.searchConditionExpression,
        ExpressionAttributeValues: opts.expressionAttributeValues,
      }),
    )
    if ((res.SearchResults ?? []).length >= opts.expectedCount) return
    if (delay > 0) await sleep(delay)
    // Grows: `delay || 500` re-evaluated to 500 on every pass, so this polled
    // at a flat 500ms and the 2000ms ceiling was unreachable.
    delay = delay === 0 ? 500 : Math.min(delay * 2, 2000)
  }
  throw new IndeterminateError(
    'vector-consistency-timeout',
    `Timeout waiting for vector index ${opts.indexName} on ${opts.tableName} to reflect ${opts.expectedCount} item(s)`,
  )
}
