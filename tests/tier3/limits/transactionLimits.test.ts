import {
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  BatchWriteItemCommand,
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
  name: uniqueTableName('lim_txn'),
  hashKey: { name: 'pk', type: 'S' },
  billingMode: 'PAY_PER_REQUEST',
}

beforeAll(async () => {
  await createTable(tableDef)
})

afterAll(async () => {
  await deleteTable(tableDef.name)
})

describe('TransactWriteItems limits', () => {
  it('TransactWriteItems with exactly 100 Put actions succeeds', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      Put: {
        TableName: tableDef.name,
        Item: { pk: { S: `tw100-${i}` }, idx: { N: String(i) } },
      },
    }))

    const result = await ddb.send(
      new TransactWriteItemsCommand({ TransactItems: items }),
    )
    expect(result).toBeDefined()
  })

  it('TransactWriteItems with 101 actions fails with ValidationException', async () => {
    const items = Array.from({ length: 101 }, (_, i) => ({
      Put: {
        TableName: tableDef.name,
        Item: { pk: { S: `tw101-${i}` }, idx: { N: String(i) } },
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
    const items = Array.from({ length: 10 }, (_, i) => ({
      Put: {
        TableName: tableDef.name,
        Item: {
          pk: { S: `tw4mb-ok-${i}` },
          payload: { S: 'x'.repeat(350_000) },
        },
      },
    }))

    const result = await ddb.send(
      new TransactWriteItemsCommand({ TransactItems: items }),
    )
    expect(result).toBeDefined()
  })

  it('TransactWriteItems total item size over 4MB fails with ValidationException', async () => {
    // 12 items of ~350KB each = ~4.2MB — over the 4MB transaction limit
    const items = Array.from({ length: 12 }, (_, i) => ({
      Put: {
        TableName: tableDef.name,
        Item: {
          pk: { S: `tw4mb-fail-${i}` },
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
    for (let batch = 0; batch < 5; batch++) {
      const requests = Array.from(
        { length: Math.min(25, 101 - batch * 25) },
        (_, i) => {
          const idx = batch * 25 + i
          return {
            PutRequest: {
              Item: { pk: { S: `tg-${idx}` }, idx: { N: String(idx) } },
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

  it('TransactGetItems with exactly 100 items succeeds', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      Get: {
        TableName: tableDef.name,
        Key: { pk: { S: `tg-${i}` } },
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
        TableName: tableDef.name,
        Key: { pk: { S: `tg-${i}` } },
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
