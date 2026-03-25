import {
  BatchWriteItemCommand,
  BatchGetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  createTable,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'
import type { TestTableDef } from '../../../src/types.js'

const tableDef: TestTableDef = {
  name: uniqueTableName('lim_batch'),
  hashKey: { name: 'pk', type: 'S' },
  billingMode: 'PAY_PER_REQUEST',
}

beforeAll(async () => {
  await createTable(tableDef)
})

afterAll(async () => {
  await deleteTable(tableDef.name)
})

describe('BatchWriteItem limits', () => {
  it('BatchWriteItem with exactly 25 items succeeds', async () => {
    const requests = Array.from({ length: 25 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `w25-${i}` }, idx: { N: String(i) } },
      },
    }))

    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [tableDef.name]: requests },
      }),
    )

    const unprocessed = result.UnprocessedItems?.[tableDef.name]
    expect(unprocessed ?? []).toHaveLength(0)
  })

  it('BatchWriteItem with 26 items fails with ValidationException', async () => {
    const requests = Array.from({ length: 26 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `w26-${i}` }, idx: { N: String(i) } },
      },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new BatchWriteItemCommand({
            RequestItems: { [tableDef.name]: requests },
          }),
        ),
      'ValidationException',
      /[Tt]oo many items|Member must have length less than or equal to 25/,
    )
  })

  it('BatchWriteItem with large items (each ~20KB, 25 items) succeeds', async () => {
    // Use 20KB per item (total ~500KB) to stay well under 16MB request limit
    const requests = Array.from({ length: 25 }, (_, i) => ({
      PutRequest: {
        Item: {
          pk: { S: `wlg-${i}` },
          payload: { S: 'x'.repeat(20_000) },
        },
      },
    }))

    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [tableDef.name]: requests },
      }),
    )

    const unprocessed = result.UnprocessedItems?.[tableDef.name]
    expect(unprocessed ?? []).toHaveLength(0)
  })
})

describe('BatchGetItem limits', () => {
  // Seed 101 items for BatchGetItem tests
  beforeAll(async () => {
    for (let batch = 0; batch < 5; batch++) {
      const requests = Array.from(
        { length: Math.min(25, 101 - batch * 25) },
        (_, i) => {
          const idx = batch * 25 + i
          return {
            PutRequest: {
              Item: { pk: { S: `g-${idx}` }, idx: { N: String(idx) } },
            },
          }
        },
      )
      if (requests.length > 0) {
        await ddb.send(
          new BatchWriteItemCommand({
            RequestItems: { [tableDef.name]: requests },
          }),
        )
      }
    }
  })

  it('BatchGetItem with exactly 100 keys succeeds', async () => {
    const keys = Array.from({ length: 100 }, (_, i) => ({
      pk: { S: `g-${i}` },
    }))

    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [tableDef.name]: { Keys: keys, ConsistentRead: true },
        },
      }),
    )

    expect(result.Responses?.[tableDef.name]).toBeDefined()
  })

  it('BatchGetItem with 101 keys fails with ValidationException', async () => {
    const keys = Array.from({ length: 101 }, (_, i) => ({
      pk: { S: `g-${i}` },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new BatchGetItemCommand({
            RequestItems: {
              [tableDef.name]: { Keys: keys, ConsistentRead: true },
            },
          }),
        ),
      'ValidationException',
      /[Tt]oo many items|Member must have length less than or equal to 100/,
    )
  })
})
