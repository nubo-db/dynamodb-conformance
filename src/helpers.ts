import {
  CreateTableCommand,
  DeleteTableCommand,
  DeleteItemCommand,
  DescribeTableCommand,
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
  timeoutMs = 60_000,
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
    await Promise.all(allNames.slice(i, i + 10).map(deleteTable))
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
}

export const hashNTableDef: TestTableDef = {
  name: uniqueTableName('hashN'),
  hashKey: { name: 'pk', type: 'N' },
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
