import { CreateTableCommand, PutItemCommand, UpdateItemCommand, DeleteItemCommand, BatchWriteItemCommand, type ConsumedCapacity } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'

// The write-side counterpart of tests/tier1/query/indexConsumedCapacity.test.ts:
// what a write to a GSI/LSI-carrying table reports under
// ReturnConsumedCapacity INDEXES. Characterised against real DynamoDB in
// eu-west-2 (2026-08-12, issue #124); every integer below is a measured value,
// all items sub-1KB so each index unit is 1 and the arithmetic isolates
// structure rather than rounding.
//
// Three measured facts the docs leave soft:
// - The top-level total folds LSI units in exactly as it does GSI units.
// - A write that costs an index nothing reports no arm at all - the
//   GlobalSecondaryIndexes / LocalSecondaryIndexes maps are absent, never a
//   zero-valued entry.
// - An identical overwrite reports the table write and no index arms: index
//   replication only charges when the write changes what the index stores.
//   Every test here therefore seeds fresh keys per attempt.
//
// A dedicated on-demand table rather than compositeIndexedTableDef: the shared
// def's indexes all key off one attribute, so a single write touches three
// indexes at once and no per-index integer can be isolated.

const tableName = uniqueTableName('idx_wcu')

let counter = 0
const freshPk = () => `item-${counter++}`

const put = (item: Record<string, { S: string }>) =>
  ddb.send(
    new PutItemCommand({
      TableName: tableName,
      Item: item,
      ReturnConsumedCapacity: 'INDEXES',
    }),
  )

function expectArms(
  cc: ConsumedCapacity | undefined,
  expected: { total: number; table: number; gsi?: number; lsi?: number },
): void {
  expect(cc?.CapacityUnits).toBe(expected.total)
  expect(cc?.Table?.CapacityUnits).toBe(expected.table)
  if (expected.gsi === undefined) {
    expect(cc?.GlobalSecondaryIndexes).toBeUndefined()
  } else {
    expect(cc?.GlobalSecondaryIndexes?.['gsi-inc']?.CapacityUnits).toBe(expected.gsi)
  }
  if (expected.lsi === undefined) {
    expect(cc?.LocalSecondaryIndexes).toBeUndefined()
  } else {
    expect(cc?.LocalSecondaryIndexes?.lsi1?.CapacityUnits).toBe(expected.lsi)
  }
}

beforeAll(async () => {
  await ddb.send(
    new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
        { AttributeName: 'gsiPk', AttributeType: 'S' },
        { AttributeName: 'lsiSk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
      GlobalSecondaryIndexes: [
        {
          IndexName: 'gsi-inc',
          KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
          // INCLUDE gives the update ladder its projected/non-projected lever.
          Projection: { ProjectionType: 'INCLUDE', NonKeyAttributes: ['proj'] },
        },
      ],
      LocalSecondaryIndexes: [
        {
          IndexName: 'lsi1',
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'lsiSk', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        },
      ],
    }),
  )
  await waitUntilActive(tableName)
})

afterAll(async () => {
  await deleteTable(tableName)
})

describe('PutItem — index write capacity', { tags: ['put-item', 'data-plane', 'gsi', 'lsi'] }, () => {
  it('reports the GSI write beside the table write and sums them', async () => {
    const res = await put({
      pk: { S: freshPk() },
      sk: { S: '1' },
      gsiPk: { S: 'g1' },
      proj: { S: 'p' },
    })
    expectArms(res.ConsumedCapacity, { total: 2, table: 1, gsi: 1 })
    // The arms carry only the aggregate; the read/write split stays absent,
    // matching the single-table behaviour pinned under issue #69.
    expect(res.ConsumedCapacity?.GlobalSecondaryIndexes?.['gsi-inc']?.WriteCapacityUnits).toBeUndefined()
  })

  it('folds the index cost into TOTAL with no breakdown arms', async () => {
    const res = await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { pk: { S: freshPk() }, sk: { S: '1' }, gsiPk: { S: 'g2' }, proj: { S: 'p' } },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )
    expect(res.ConsumedCapacity?.CapacityUnits).toBe(2)
    expect(res.ConsumedCapacity?.Table).toBeUndefined()
    expect(res.ConsumedCapacity?.GlobalSecondaryIndexes).toBeUndefined()
  })

  it('charges a sparse write nothing for the indexes it misses', async () => {
    const res = await put({ pk: { S: freshPk() }, sk: { S: '1' }, other: { S: 'o' } })
    expectArms(res.ConsumedCapacity, { total: 1, table: 1 })
  })

  it('reports the LSI write and folds it into the total', async () => {
    const res = await put({ pk: { S: freshPk() }, sk: { S: '1' }, lsiSk: { S: 'L1' } })
    expectArms(res.ConsumedCapacity, { total: 2, table: 1, lsi: 1 })
  })

  it('reports both arms when a write touches both index kinds', async () => {
    const res = await put({
      pk: { S: freshPk() },
      sk: { S: '1' },
      gsiPk: { S: 'g3' },
      lsiSk: { S: 'L1' },
      proj: { S: 'p' },
    })
    expectArms(res.ConsumedCapacity, { total: 3, table: 1, gsi: 1, lsi: 1 })
  })

  it('charges an identical overwrite no index writes at all', async () => {
    const item = {
      pk: { S: freshPk() },
      sk: { S: '1' },
      gsiPk: { S: 'g4' },
      lsiSk: { S: 'L1' },
      proj: { S: 'p' },
    }
    const first = await put(item)
    expectArms(first.ConsumedCapacity, { total: 3, table: 1, gsi: 1, lsi: 1 })
    const repeat = await put(item)
    expectArms(repeat.ConsumedCapacity, { total: 1, table: 1 })
  })
})

describe('UpdateItem — index write-capacity ladder', { tags: ['update-item', 'data-plane', 'gsi', 'lsi'] }, () => {
  // Each rung seeds its own item so a retried test never mutates state a
  // previous attempt left behind.
  async function seed(): Promise<{ pk: string }> {
    const pk = freshPk()
    await put({
      pk: { S: pk },
      sk: { S: '1' },
      gsiPk: { S: 'g-ladder' },
      lsiSk: { S: 'L1' },
      proj: { S: 'p' },
      other: { S: 'o' },
    })
    return { pk }
  }

  const update = (
    pk: string,
    expr: string,
    values?: Record<string, { S: string }>,
    names?: Record<string, string>,
  ) =>
    ddb.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { pk: { S: pk }, sk: { S: '1' } },
        UpdateExpression: expr,
        ExpressionAttributeValues: values,
        ExpressionAttributeNames: names,
        ReturnConsumedCapacity: 'INDEXES',
      }),
    )

  it('charges nothing on the index for a non-projected attribute', async () => {
    const { pk } = await seed()
    const res = await update(pk, 'SET #o = :v', { ':v': { S: 'o2' } }, { '#o': 'other' })
    expectArms(res.ConsumedCapacity, { total: 2, table: 1, lsi: 1 })
  })

  it('charges one index write for a projected non-key attribute', async () => {
    const { pk } = await seed()
    const res = await update(pk, 'SET proj = :v', { ':v': { S: 'p2' } })
    expectArms(res.ConsumedCapacity, { total: 3, table: 1, gsi: 1, lsi: 1 })
  })

  it('charges two index writes for a key change: delete plus insert', async () => {
    const { pk } = await seed()
    const res = await update(pk, 'SET gsiPk = :v', { ':v': { S: 'g-moved' } })
    expectArms(res.ConsumedCapacity, { total: 4, table: 1, gsi: 2, lsi: 1 })
  })

  it('charges one index write to remove the key: delete only', async () => {
    const { pk } = await seed()
    const res = await update(pk, 'REMOVE gsiPk')
    expectArms(res.ConsumedCapacity, { total: 3, table: 1, gsi: 1, lsi: 1 })
  })

  it('walks the same ladder on the LSI key', async () => {
    const { pk } = await seed()
    const res = await update(pk, 'SET lsiSk = :v', { ':v': { S: 'L2' } })
    // No GSI arm: lsiSk is outside gsi-inc's INCLUDE projection, so the GSI's
    // stored view of the item never changed. The mirror case holds above -
    // the gsiPk change charged the ALL-projected LSI one unit.
    expectArms(res.ConsumedCapacity, { total: 3, table: 1, lsi: 2 })
  })
})

describe('DeleteItem — index write capacity', { tags: ['delete-item', 'data-plane', 'gsi', 'lsi'] }, () => {
  it('charges one write per index the item occupied', async () => {
    const pk = freshPk()
    await put({
      pk: { S: pk },
      sk: { S: '1' },
      gsiPk: { S: 'g-del' },
      lsiSk: { S: 'L1' },
      proj: { S: 'p' },
    })
    const res = await ddb.send(
      new DeleteItemCommand({
        TableName: tableName,
        Key: { pk: { S: pk }, sk: { S: '1' } },
        ReturnConsumedCapacity: 'INDEXES',
      }),
    )
    expectArms(res.ConsumedCapacity, { total: 3, table: 1, gsi: 1, lsi: 1 })
  })

  it('charges only the table write for a sparse item', async () => {
    const pk = freshPk()
    await put({ pk: { S: pk }, sk: { S: '1' }, other: { S: 'o' } })
    const res = await ddb.send(
      new DeleteItemCommand({
        TableName: tableName,
        Key: { pk: { S: pk }, sk: { S: '1' } },
        ReturnConsumedCapacity: 'INDEXES',
      }),
    )
    expectArms(res.ConsumedCapacity, { total: 1, table: 1 })
  })
})

describe('BatchWriteItem — index write capacity', { tags: ['batch', 'data-plane', 'gsi', 'lsi'] }, () => {
  it('reports the per-table entry with the arms the batch actually touched', async () => {
    const indexed = freshPk()
    const sparse = freshPk()
    const res = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [tableName]: [
            {
              PutRequest: {
                Item: {
                  pk: { S: indexed },
                  sk: { S: '1' },
                  gsiPk: { S: 'g-batch' },
                  proj: { S: 'p' },
                },
              },
            },
            { PutRequest: { Item: { pk: { S: sparse }, sk: { S: '1' }, other: { S: 'o' } } } },
          ],
        },
        ReturnConsumedCapacity: 'INDEXES',
      }),
    )
    expect(res.UnprocessedItems).toEqual({})
    expect(res.ConsumedCapacity).toHaveLength(1)
    expectArms(res.ConsumedCapacity![0], { total: 3, table: 2, gsi: 1 })
  })
})
