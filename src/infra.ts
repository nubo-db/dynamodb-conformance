// Provisioning and teardown helpers for the control-plane operations that
// depend on real cloud infrastructure (S3 export/import, Kinesis streaming
// destinations, multi-region replicas), plus a support probe that classifies
// "operation not implemented" so dependent tests skip on emulators that lack
// the operation.
//
// Every `with*` helper guarantees teardown in a `finally`, so no billable
// resource is left behind. Teardown is idempotent and swallows not-found, the
// same posture as `deleteTable` in helpers.ts.
//
// NOTE: these helpers are typed against the SDK but their behaviour against real
// AWS is verified by the control-plane characterisation tests, which are the
// first thing to exercise them. Until those run, treat the AWS-specific details
// (S3 bucket location constraint, Kinesis wait timing, the probe fault
// classifier) as provisional.

import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  UpdateContinuousBackupsCommand,
  ResourceNotFoundException,
  type CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb'
import {
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  ListObjectsV2Command,
  type CreateBucketCommandInput,
  type BucketLocationConstraint,
} from '@aws-sdk/client-s3'
import {
  CreateStreamCommand,
  DeleteStreamCommand,
  DescribeStreamSummaryCommand,
  waitUntilStreamExists,
} from '@aws-sdk/client-kinesis'
import { s3, kinesis } from './aws-aux.js'
import { isEmulator, region } from './aws-config.js'

let resourceCounter = 0
function uniqueName(base: string): string {
  return `conformance-${base}-${Date.now()}-${resourceCounter++}`
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// ── Control-plane support probe ───────────────────────────────────────────

/**
 * Classify an error as "the target does not implement this operation". A real
 * error (validation, not-found, access-denied) means the operation *is*
 * implemented and the test should assert, not skip. The characterisation tests
 * refine the exact fault shapes each target uses.
 */
export function isUnsupportedFault(err: unknown): boolean {
  const e = err as { name?: string; message?: string; $metadata?: { httpStatusCode?: number } }
  const name = e?.name ?? ''
  const message = e?.message ?? ''
  const status = e?.$metadata?.httpStatusCode
  return (
    name === 'UnknownOperationException' ||
    /unknown operation|not implemented|unsupported operation|is not supported/i.test(message) ||
    status === 501
  )
}

/**
 * Probe whether the target implements a control-plane operation. Returns true
 * if the call succeeds or fails with a real error; false only if the target
 * signals the operation is unimplemented. Use in `beforeAll` to drive the
 * feature-probe skip pattern.
 */
export async function supportsControlPlaneOp(
  fn: () => Promise<unknown>,
): Promise<boolean> {
  try {
    await fn()
    return true
  } catch (e) {
    return !isUnsupportedFault(e)
  }
}

// ── S3 (export / import) ────────────────────────────────────────────────────

/** Empty and delete an S3 bucket. Idempotent; swallows errors. */
export async function emptyAndDeleteBucket(bucket: string): Promise<void> {
  try {
    let token: string | undefined
    do {
      const listed = await s3.send(
        new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: token }),
      )
      const objects = (listed.Contents ?? [])
        .map((o) => o.Key)
        .filter((k): k is string => typeof k === 'string')
        .map((Key) => ({ Key }))
      if (objects.length > 0) {
        await s3.send(
          new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: objects } }),
        )
      }
      token = listed.IsTruncated ? listed.NextContinuationToken : undefined
    } while (token)
    await s3.send(new DeleteBucketCommand({ Bucket: bucket }))
  } catch {
    // Best-effort teardown.
  }
}

/**
 * Provision an S3 bucket, run `fn`, then empty and delete the bucket. Objects
 * are always removed; the bucket itself is deleted too (an empty bucket is the
 * only acceptable residue if deletion races).
 */
export async function withS3Bucket<T>(
  fn: (bucket: string) => Promise<T>,
): Promise<T> {
  const bucket = uniqueName('export').toLowerCase()
  const input: CreateBucketCommandInput = { Bucket: bucket }
  // Outside us-east-1, real S3 requires an explicit location constraint.
  if (!isEmulator && region !== 'us-east-1') {
    input.CreateBucketConfiguration = {
      LocationConstraint: region as BucketLocationConstraint,
    }
  }
  await s3.send(new CreateBucketCommand(input))
  try {
    return await fn(bucket)
  } finally {
    await emptyAndDeleteBucket(bucket)
  }
}

// ── Kinesis (streaming destination) ──────────────────────────────────────────

/**
 * Provision a single-shard Kinesis stream, wait until it is active, run `fn`
 * with the stream ARN, then delete the stream.
 */
export async function withKinesisStream<T>(
  fn: (streamArn: string, streamName: string) => Promise<T>,
): Promise<T> {
  const name = uniqueName('stream')
  await kinesis.send(new CreateStreamCommand({ StreamName: name, ShardCount: 1 }))
  await waitUntilStreamExists(
    { client: kinesis, maxWaitTime: 120 },
    { StreamName: name },
  )
  const summary = await kinesis.send(
    new DescribeStreamSummaryCommand({ StreamName: name }),
  )
  const arn = summary.StreamDescriptionSummary?.StreamARN
  if (!arn) throw new Error(`Kinesis stream ${name} has no ARN`)
  try {
    return await fn(arn, name)
  } finally {
    try {
      await kinesis.send(
        new DeleteStreamCommand({ StreamName: name, EnforceConsumerDeletion: true }),
      )
    } catch {
      // Best-effort teardown.
    }
  }
}

// ── Region-aware DynamoDB table lifecycle (global tables) ────────────────────

/** Wait until a table is ACTIVE in a specific region's client. */
export async function waitUntilActiveInRegion(
  client: DynamoDBClient,
  tableName: string,
  timeoutMs = 60_000,
): Promise<void> {
  const start = Date.now()
  let delay = 0
  while (Date.now() - start < timeoutMs) {
    const res = await client.send(new DescribeTableCommand({ TableName: tableName }))
    if (res.Table?.TableStatus === 'ACTIVE') return
    if (delay > 0) await sleep(delay)
    delay = Math.min(delay || 500, 2000)
  }
  throw new Error(`Timeout waiting for table ${tableName} to become ACTIVE`)
}

/** Create a table in a specific region and wait for it to become ACTIVE. */
export async function createTableInRegion(
  client: DynamoDBClient,
  input: CreateTableCommandInput,
): Promise<void> {
  await client.send(new CreateTableCommand(input))
  await waitUntilActiveInRegion(client, input.TableName!)
}

/** Delete a table in a specific region. Idempotent; swallows not-found. */
export async function deleteTableInRegion(
  client: DynamoDBClient,
  tableName: string,
): Promise<void> {
  try {
    await client.send(new DeleteTableCommand({ TableName: tableName }))
  } catch (e) {
    if (e instanceof ResourceNotFoundException) return
    // Best-effort teardown for other transient states.
  }
}

// ── Point-in-time recovery teardown ──────────────────────────────────────────

/** Disable PITR on a table. Idempotent; swallows errors. */
export async function disablePitr(
  client: DynamoDBClient,
  tableName: string,
): Promise<void> {
  try {
    await client.send(
      new UpdateContinuousBackupsCommand({
        TableName: tableName,
        PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: false },
      }),
    )
  } catch {
    // Best-effort teardown.
  }
}
