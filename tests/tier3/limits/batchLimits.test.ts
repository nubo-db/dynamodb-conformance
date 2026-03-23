import {
  BatchWriteItemCommand,
  BatchGetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const PREFIX = 'lim-batch-'
const keysToClean: { pk: { S: string } }[] = []

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToClean)
})

function trackKey(id: string) {
  const k = { pk: { S: `${PREFIX}${id}` } }
  keysToClean.push(k)
  return k
}

describe('BatchWriteItem limits', () => {
  it('BatchWriteItem with exactly 25 items succeeds', async () => {
    const requests = Array.from({ length: 25 }, (_, i) => {
      trackKey(`w25-${i}`)
      return {
        PutRequest: {
          Item: { pk: { S: `${PREFIX}w25-${i}` }, idx: { N: String(i) } },
        },
      }
    })

    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: requests },
      }),
    )

    const unprocessed = result.UnprocessedItems?.[hashTableDef.name]
    expect(unprocessed ?? []).toHaveLength(0)
  })

  it('BatchWriteItem with 26 items fails with ValidationException', async () => {
    const requests = Array.from({ length: 26 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `${PREFIX}w26-${i}` }, idx: { N: String(i) } },
      },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new BatchWriteItemCommand({
            RequestItems: { [hashTableDef.name]: requests },
          }),
        ),
      'ValidationException',
      /[Tt]oo many items|Member must have length less than or equal to 25/,
    )
  })

  it('BatchWriteItem with large items (each ~20KB, 25 items) succeeds', async () => {
    // Use 20KB per item (total ~500KB) to stay well under 16MB request limit
    const requests = Array.from({ length: 25 }, (_, i) => {
      trackKey(`wlg-${i}`)
      return {
        PutRequest: {
          Item: {
            pk: { S: `${PREFIX}wlg-${i}` },
            payload: { S: 'x'.repeat(20_000) },
          },
        },
      }
    })

    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: requests },
      }),
    )

    const unprocessed = result.UnprocessedItems?.[hashTableDef.name]
    expect(unprocessed ?? []).toHaveLength(0)
  })
})

describe('BatchGetItem limits', () => {
  // Seed 101 items for BatchGetItem tests
  beforeAll(async () => {
    // Write in batches of 25
    for (let batch = 0; batch < 5; batch++) {
      const requests = Array.from(
        { length: Math.min(25, 101 - batch * 25) },
        (_, i) => {
          const idx = batch * 25 + i
          trackKey(`g-${idx}`)
          return {
            PutRequest: {
              Item: { pk: { S: `${PREFIX}g-${idx}` }, idx: { N: String(idx) } },
            },
          }
        },
      )
      if (requests.length > 0) {
        await ddb.send(
          new BatchWriteItemCommand({
            RequestItems: { [hashTableDef.name]: requests },
          }),
        )
      }
    }
  })

  it('BatchGetItem with exactly 100 keys succeeds', async () => {
    const keys = Array.from({ length: 100 }, (_, i) => ({
      pk: { S: `${PREFIX}g-${i}` },
    }))

    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: { Keys: keys, ConsistentRead: true },
        },
      }),
    )

    expect(result.Responses?.[hashTableDef.name]).toBeDefined()
  })

  it('BatchGetItem with 101 keys fails with ValidationException', async () => {
    const keys = Array.from({ length: 101 }, (_, i) => ({
      pk: { S: `${PREFIX}g-${i}` },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new BatchGetItemCommand({
            RequestItems: {
              [hashTableDef.name]: { Keys: keys, ConsistentRead: true },
            },
          }),
        ),
      'ValidationException',
      /[Tt]oo many items|Member must have length less than or equal to 100/,
    )
  })
})
