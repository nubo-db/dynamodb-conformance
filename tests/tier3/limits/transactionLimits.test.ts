import {
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  BatchWriteItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const PREFIX = 'lim-txn-'
const keysToClean: { pk: { S: string } }[] = []

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToClean)
})

function trackKey(id: string) {
  const k = { pk: { S: `${PREFIX}${id}` } }
  keysToClean.push(k)
  return k
}

describe('TransactWriteItems limits', () => {
  it('TransactWriteItems with exactly 100 Put actions succeeds', async () => {
    const items = Array.from({ length: 100 }, (_, i) => {
      trackKey(`tw100-${i}`)
      return {
        Put: {
          TableName: hashTableDef.name,
          Item: { pk: { S: `${PREFIX}tw100-${i}` }, idx: { N: String(i) } },
        },
      }
    })

    const result = await ddb.send(
      new TransactWriteItemsCommand({ TransactItems: items }),
    )
    expect(result).toBeDefined()
  })

  it('TransactWriteItems with 101 actions fails with ValidationException', async () => {
    const items = Array.from({ length: 101 }, (_, i) => ({
      Put: {
        TableName: hashTableDef.name,
        Item: { pk: { S: `${PREFIX}tw101-${i}` }, idx: { N: String(i) } },
      },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new TransactWriteItemsCommand({ TransactItems: items }),
        ),
      'ValidationException',
      /[Mm]ember must have length less than or equal to 100/,
    )
  })

  it('TransactWriteItems total item size approaching 4MB succeeds', async () => {
    // 10 items of ~350KB each = ~3.5MB — under the 4MB transaction limit
    const items = Array.from({ length: 10 }, (_, i) => {
      trackKey(`tw4mb-ok-${i}`)
      return {
        Put: {
          TableName: hashTableDef.name,
          Item: {
            pk: { S: `${PREFIX}tw4mb-ok-${i}` },
            payload: { S: 'x'.repeat(350_000) },
          },
        },
      }
    })

    const result = await ddb.send(
      new TransactWriteItemsCommand({ TransactItems: items }),
    )
    expect(result).toBeDefined()
  })

  it('TransactWriteItems total item size over 4MB fails with ValidationException', async () => {
    // 12 items of ~350KB each = ~4.2MB — over the 4MB transaction limit
    const items = Array.from({ length: 12 }, (_, i) => ({
      Put: {
        TableName: hashTableDef.name,
        Item: {
          pk: { S: `${PREFIX}tw4mb-fail-${i}` },
          payload: { S: 'x'.repeat(350_000) },
        },
      },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new TransactWriteItemsCommand({ TransactItems: items }),
        ),
      'ValidationException',
    )
  })
})

describe('TransactGetItems limits', () => {
  // Seed 101 items for TransactGetItems tests
  beforeAll(async () => {
    // Write in batches of 25, with sleeps to avoid ProvisionedThroughputExceededException
    for (let batch = 0; batch < 5; batch++) {
      if (batch > 0) await new Promise((r) => setTimeout(r, 6_000))
      const requests = Array.from(
        { length: Math.min(25, 101 - batch * 25) },
        (_, i) => {
          const idx = batch * 25 + i
          trackKey(`tg-${idx}`)
          return {
            PutRequest: {
              Item: { pk: { S: `${PREFIX}tg-${idx}` }, idx: { N: String(idx) } },
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

  it('TransactGetItems with exactly 100 items succeeds', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      Get: {
        TableName: hashTableDef.name,
        Key: { pk: { S: `${PREFIX}tg-${i}` } },
      },
    }))

    const result = await ddb.send(
      new TransactGetItemsCommand({ TransactItems: items }),
    )
    expect(result.Responses).toBeDefined()
    expect(result.Responses).toHaveLength(100)
  })

  it('TransactGetItems with 101 items fails with ValidationException', async () => {
    const items = Array.from({ length: 101 }, (_, i) => ({
      Get: {
        TableName: hashTableDef.name,
        Key: { pk: { S: `${PREFIX}tg-${i}` } },
      },
    }))

    await expectDynamoError(
      () =>
        ddb.send(
          new TransactGetItemsCommand({ TransactItems: items }),
        ),
      'ValidationException',
      /[Mm]ember must have length less than or equal to 100/,
    )
  })
})
