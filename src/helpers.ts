import {
  CreateTableCommand,
  DeleteTableCommand,
  DeleteItemCommand,
  DescribeTableCommand,
  UpdateTableCommand,
  UpdateContinuousBackupsCommand,
  ListTablesCommand,
  QueryCommand,
  DynamoDBServiceException,
  ResourceInUseException,
  ResourceNotFoundException,
  type CreateTableCommandInput,
  type AttributeDefinition,
  type KeySchemaElement,
  type GlobalSecondaryIndex,
  type LocalSecondaryIndex,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb'
import { ddb } from './client.js'
import type { TestTableDef } from './types.js'

const TABLE_PREFIX = '_conformance_'
let counter = 0

/** Generate a unique table name for this test run */
export function uniqueTableName(base: string): string {
  return `${TABLE_PREFIX}${base}_${Date.now()}_${counter++}`
}

/** Build a CreateTableCommand input from a TestTableDef */
function buildCreateInput(def: TestTableDef): CreateTableCommandInput {
  const attrs: AttributeDefinition[] = [
    { AttributeName: def.hashKey.name, AttributeType: def.hashKey.type },
  ]
  const keySchema: KeySchemaElement[] = [
    { AttributeName: def.hashKey.name, KeyType: 'HASH' },
  ]

  if (def.rangeKey) {
    attrs.push({
      AttributeName: def.rangeKey.name,
      AttributeType: def.rangeKey.type,
    })
    keySchema.push({ AttributeName: def.rangeKey.name, KeyType: 'RANGE' })
  }

  const gsis: GlobalSecondaryIndex[] = []
  if (def.gsis) {
    for (const g of def.gsis) {
      if (!attrs.find((a) => a.AttributeName === g.hashKey.name)) {
        attrs.push({
          AttributeName: g.hashKey.name,
          AttributeType: g.hashKey.type,
        })
      }
      const gsiKeySchema: KeySchemaElement[] = [
        { AttributeName: g.hashKey.name, KeyType: 'HASH' },
      ]
      if (g.rangeKey) {
        if (!attrs.find((a) => a.AttributeName === g.rangeKey!.name)) {
          attrs.push({
            AttributeName: g.rangeKey.name,
            AttributeType: g.rangeKey.type,
          })
        }
        gsiKeySchema.push({ AttributeName: g.rangeKey.name, KeyType: 'RANGE' })
      }
      gsis.push({
        IndexName: g.indexName,
        KeySchema: gsiKeySchema,
        Projection: {
          ProjectionType: g.projectionType,
          ...(g.nonKeyAttributes
            ? { NonKeyAttributes: g.nonKeyAttributes }
            : {}),
        },
        ...(def.billingMode !== 'PAY_PER_REQUEST'
          ? {
              ProvisionedThroughput: {
                ReadCapacityUnits: 5,
                WriteCapacityUnits: 5,
              },
            }
          : {}),
      })
    }
  }

  const lsis: LocalSecondaryIndex[] = []
  if (def.lsis) {
    for (const l of def.lsis) {
      if (!attrs.find((a) => a.AttributeName === l.rangeKey.name)) {
        attrs.push({
          AttributeName: l.rangeKey.name,
          AttributeType: l.rangeKey.type,
        })
      }
      lsis.push({
        IndexName: l.indexName,
        KeySchema: [
          { AttributeName: def.hashKey.name, KeyType: 'HASH' },
          { AttributeName: l.rangeKey.name, KeyType: 'RANGE' },
        ],
        Projection: {
          ProjectionType: l.projectionType,
          ...(l.nonKeyAttributes
            ? { NonKeyAttributes: l.nonKeyAttributes }
            : {}),
        },
      })
    }
  }

  return {
    TableName: def.name,
    AttributeDefinitions: attrs,
    KeySchema: keySchema,
    ...(gsis.length ? { GlobalSecondaryIndexes: gsis } : {}),
    ...(lsis.length ? { LocalSecondaryIndexes: lsis } : {}),
    ...(def.billingMode === 'PAY_PER_REQUEST'
      ? { BillingMode: 'PAY_PER_REQUEST' as const }
      : {
          ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5,
          },
        }),
  }
}

/** Wait until a table reaches ACTIVE status (and all GSIs are ACTIVE) */
export async function waitUntilActive(
  tableName: string,
  timeoutMs = 120_000,
): Promise<void> {
  const start = Date.now()
  let delay = 0
  while (Date.now() - start < timeoutMs) {
    const res = await ddb.send(
      new DescribeTableCommand({ TableName: tableName }),
    )
    const table = res.Table
    if (!table) throw new Error(`DescribeTable returned no Table for ${tableName}`)

    const tableActive = table.TableStatus === 'ACTIVE'
    const gsisActive =
      !table.GlobalSecondaryIndexes ||
      table.GlobalSecondaryIndexes.every((i) => i.IndexStatus === 'ACTIVE')

    if (tableActive && gsisActive) return

    if (delay > 0) await sleep(delay)
    delay = Math.min(delay || 500, 2000)
  }
  throw new Error(`Timeout waiting for table ${tableName} to become ACTIVE`)
}

/** Create a table from a TestTableDef and wait for it to become ACTIVE */
export async function createTable(def: TestTableDef): Promise<void> {
  const input = buildCreateInput(def)
  await ddb.send(new CreateTableCommand(input))
  await waitUntilActive(def.name)
}

/** Delete a table, ignoring ResourceNotFoundException and ResourceInUseException */
export async function deleteTable(tableName: string): Promise<void> {
  try {
    await ddb.send(new DeleteTableCommand({ TableName: tableName }))
    // Wait until gone
    const start = Date.now()
    let delay = 0
    while (Date.now() - start < 30_000) {
      try {
        await ddb.send(new DescribeTableCommand({ TableName: tableName }))
        if (delay > 0) await sleep(delay)
        delay = Math.min(delay || 500, 2000)
      } catch (e: unknown) {
        if (e instanceof ResourceNotFoundException) return
        throw e
      }
    }
  } catch (e: unknown) {
    if (e instanceof ResourceNotFoundException) return
    if (e instanceof ResourceInUseException) return // already being deleted
    // A deletion-protected table cannot be deleted until protection is
    // disabled. Disable and retry once so cleanup is robust.
    if (
      e instanceof DynamoDBServiceException &&
      e.name === 'ValidationException' &&
      /protected against deletion|deletion protection is enabled/i.test(e.message)
    ) {
      // Best-effort: disable protection then delete. A target that blocks the
      // delete but rejects the protection toggle (some emulators) must not make
      // cleanup throw and poison the run.
      try {
        await disableDeletionProtection(tableName)
        await waitUntilActive(tableName)
        await ddb.send(new DeleteTableCommand({ TableName: tableName }))
      } catch {
        // give up; cleanup is best-effort
      }
      return
    }
    throw e
  }
}

/**
 * Retry an operation while DynamoDB is still enabling continuous backups on a
 * freshly-created table (CreateBackup and UpdateContinuousBackups both throw
 * ContinuousBackupsUnavailableException during that window).
 */
export async function retryWhileBackupsEnabling<T>(
  fn: () => Promise<T>,
  timeoutMs = 120_000,
): Promise<T> {
  const start = Date.now()
  let delay = 0
  for (;;) {
    try {
      return await fn()
    } catch (e: unknown) {
      if (
        e instanceof DynamoDBServiceException &&
        e.name === 'ContinuousBackupsUnavailableException' &&
        Date.now() - start < timeoutMs
      ) {
        if (delay > 0) await sleep(delay)
        delay = Math.min(delay || 2000, 5000)
        continue
      }
      throw e
    }
  }
}

/** Enable point-in-time recovery, waiting out the post-create enabling window. */
export async function enablePitr(tableName: string): Promise<void> {
  await retryWhileBackupsEnabling(() =>
    ddb.send(
      new UpdateContinuousBackupsCommand({
        TableName: tableName,
        PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
      }),
    ),
  )
}

/**
 * Disable deletion protection, absorbing DynamoDB's throttle on changing the
 * deletion-protection setting more than once per 15 seconds.
 */
async function disableDeletionProtection(tableName: string): Promise<void> {
  try {
    await ddb.send(
      new UpdateTableCommand({ TableName: tableName, DeletionProtectionEnabled: false }),
    )
  } catch (e: unknown) {
    if (e instanceof DynamoDBServiceException && e.name === 'ThrottlingException') {
      await sleep(16_000)
      await ddb.send(
        new UpdateTableCommand({ TableName: tableName, DeletionProtectionEnabled: false }),
      )
      return
    }
    throw e
  }
}

/** Delete all tables created by the conformance suite */
export async function cleanupAllTables(): Promise<void> {
  const allNames: string[] = []
  let exclusiveStartTableName: string | undefined
  do {
    const res = await ddb.send(
      new ListTablesCommand({ ExclusiveStartTableName: exclusiveStartTableName }),
    )
    const names = (res.TableNames ?? []).filter((n) => n.startsWith(TABLE_PREFIX))
    allNames.push(...names)
    exclusiveStartTableName = res.LastEvaluatedTableName
  } while (exclusiveStartTableName)

  for (let i = 0; i < allNames.length; i += 10) {
    // Best-effort: one undeletable table (e.g. a deletion-protected table on a
    // target that won't toggle protection off) must not poison setup.
    await Promise.all(
      allNames.slice(i, i + 10).map((n) => deleteTable(n).catch(() => {})),
    )
  }
}

/** Assert that a DynamoDB error has the expected name and message pattern */
export function assertDynamoError(
  error: unknown,
  expectedName: string,
  expectedMessage?: string | RegExp,
): void {
  expect(error).toBeDefined()
  expect(error).toBeInstanceOf(DynamoDBServiceException)
  const err = error as DynamoDBServiceException
  expect(err.name).toBe(expectedName)
  if (expectedMessage) {
    if (typeof expectedMessage === 'string') {
      expect(err.message).toContain(expectedMessage)
    } else {
      expect(err.message).toMatch(expectedMessage)
    }
  }
}

/** Expect an async function to throw a DynamoDB error with the given name/message */
export async function expectDynamoError(
  fn: () => Promise<unknown>,
  expectedName: string,
  expectedMessage?: string | RegExp,
): Promise<void> {
  try {
    await fn()
    expect.unreachable('should have thrown')
  } catch (e) {
    assertDynamoError(e, expectedName, expectedMessage)
  }
}

/** Delete a batch of items by key, ignoring errors */
export async function cleanupItems(
  tableName: string,
  keys: Record<string, AttributeValue>[],
): Promise<void> {
  await Promise.all(
    keys.map((key) =>
      ddb.send(new DeleteItemCommand({ TableName: tableName, Key: key })).catch(() => {}),
    ),
  )
}

interface WaitForGsiOptions {
  tableName: string
  indexName: string
  partitionKey: { name: string; value: AttributeValue }
  expectedCount: number
  timeoutMs?: number
}

/** Wait for GSI to reflect the expected number of items */
export async function waitForGsiConsistency(opts: WaitForGsiOptions): Promise<void> {
  const { tableName, indexName, partitionKey, expectedCount, timeoutMs = 10_000 } = opts
  const start = Date.now()
  let delay = 0
  while (Date.now() - start < timeoutMs) {
    const res = await ddb.send(
      new QueryCommand({
        TableName: tableName,
        IndexName: indexName,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': partitionKey.name },
        ExpressionAttributeValues: { ':pk': partitionKey.value },
      }),
    )
    if ((res.Count ?? 0) >= expectedCount) return
    if (delay > 0) await sleep(delay)
    delay = Math.min((delay || 250) * 1.5, 2000)
  }
  throw new Error(`Timeout waiting for GSI ${indexName} consistency (expected ${expectedCount} items)`)
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// ── Shared table definitions ──────────────────────────────────────────
// These mirror the dynalite test tables so we can port tests directly.

export const hashTableDef: TestTableDef = {
  name: uniqueTableName('hash'),
  hashKey: { name: 'pk', type: 'S' },
  // On-demand, like hashNTableDef. The shared tables now live for the whole run
  // (provisioned once), so a provisioned 5-WCU table runs out of burst capacity
  // for the near-400KB writes in tests/tier3/limits/itemSize.test.ts and throttles.
  // Nothing asserts this table is provisioned; createTable/config covers that mode.
  billingMode: 'PAY_PER_REQUEST',
}

export const hashNTableDef: TestTableDef = {
  name: uniqueTableName('hashN'),
  hashKey: { name: 'pk', type: 'N' },
  billingMode: 'PAY_PER_REQUEST',
}

// Binary partition key. The other shared defs are string- or number-keyed, so an
// empty-binary key value only reaches the binary key validator on a table whose
// key attribute is declared type B. Used by the empty-binary key-value cases,
// which pin the `Key: pk` message real AWS returns for a zero-length binary key.
export const hashBTableDef: TestTableDef = {
  name: uniqueTableName('hashB'),
  hashKey: { name: 'pk', type: 'B' },
  billingMode: 'PAY_PER_REQUEST',
}

export const compositeTableDef: TestTableDef = {
  name: uniqueTableName('composite'),
  hashKey: { name: 'pk', type: 'S' },
  rangeKey: { name: 'sk', type: 'S' },
  lsis: [
    {
      indexName: 'lsi1',
      rangeKey: { name: 'lsi1sk', type: 'S' },
      projectionType: 'ALL',
    },
    {
      indexName: 'lsi2',
      rangeKey: { name: 'lsi2sk', type: 'S' },
      projectionType: 'INCLUDE',
      nonKeyAttributes: ['lsi1sk'],
    },
  ],
  gsis: [
    {
      indexName: 'gsi1',
      hashKey: { name: 'lsi1sk', type: 'S' },
      projectionType: 'ALL',
    },
    {
      indexName: 'gsi2',
      hashKey: { name: 'lsi1sk', type: 'S' },
      rangeKey: { name: 'lsi2sk', type: 'S' },
      projectionType: 'INCLUDE',
      nonKeyAttributes: ['data'],
    },
  ],
}

export const compositeNTableDef: TestTableDef = {
  name: uniqueTableName('compositeN'),
  hashKey: { name: 'pk', type: 'S' },
  rangeKey: { name: 'sk', type: 'N' },
}

export const compositeBTableDef: TestTableDef = {
  name: uniqueTableName('compositeB'),
  hashKey: { name: 'pk', type: 'S' },
  rangeKey: { name: 'sk', type: 'B' },
}
