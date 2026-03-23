import {
  BatchExecuteStatementCommand,
  ExecuteStatementCommand,
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('BatchExecuteStatement — PartiQL', () => {
  let supported = true

  const keysToCleanup: Record<string, { S: string }>[] = []

  beforeAll(async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-canary'`,
      }))
    } catch (e: unknown) {
      if (e instanceof Error && (e.name === 'UnknownOperationException' || e.name === 'UnrecognizedClientException')) {
        supported = false
      }
    }
  })

  beforeEach(({ skip }) => { if (!supported) skip() })

  afterAll(async () => {
    if (keysToCleanup.length > 0) {
      await cleanupItems(hashTableDef.name, keysToCleanup)
    }
  })

  it('batch of multiple SELECT statements', async () => {
    keysToCleanup.push(
      { pk: { S: 'batch-sel-1' } },
      { pk: { S: 'batch-sel-2' } },
    )

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'batch-sel-1' }, data: { S: 'one' } },
    }))
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'batch-sel-2' }, data: { S: 'two' } },
    }))

    const result = await ddb.send(new BatchExecuteStatementCommand({
      Statements: [
        { Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'batch-sel-1'` },
        { Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'batch-sel-2'` },
      ],
    }))

    expect(result.Responses).toBeDefined()
    expect(result.Responses!.length).toBe(2)

    const items = result.Responses!.map(r => r.Item)
    const pks = items.map(i => i?.pk.S).sort()
    expect(pks).toEqual(['batch-sel-1', 'batch-sel-2'])
  })

  it('batch of INSERT and UPDATE statements', async () => {
    keysToCleanup.push(
      { pk: { S: 'batch-ins-1' } },
      { pk: { S: 'batch-upd-1' } },
    )

    // Seed an item for the UPDATE
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'batch-upd-1' }, data: { S: 'before' } },
    }))

    const result = await ddb.send(new BatchExecuteStatementCommand({
      Statements: [
        { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'batch-ins-1', 'data': 'new'}` },
        { Statement: `UPDATE "${hashTableDef.name}" SET data = 'after' WHERE pk = 'batch-upd-1'` },
      ],
    }))

    expect(result.Responses).toBeDefined()
    expect(result.Responses!.length).toBe(2)

    // Verify the INSERT
    const inserted = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'batch-ins-1' } },
      ConsistentRead: true,
    }))
    expect(inserted.Item).toBeDefined()
    expect(inserted.Item!.data.S).toBe('new')

    // Verify the UPDATE
    const updated = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'batch-upd-1' } },
      ConsistentRead: true,
    }))
    expect(updated.Item).toBeDefined()
    expect(updated.Item!.data.S).toBe('after')
  })

  it('partial failure — one valid and one invalid statement', async () => {
    keysToCleanup.push({ pk: { S: 'batch-partial-1' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'batch-partial-1' }, data: { S: 'exists' } },
    }))

    const result = await ddb.send(new BatchExecuteStatementCommand({
      Statements: [
        { Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'batch-partial-1'` },
        { Statement: `SELECT * FROM "table_that_does_not_exist_xyz" WHERE pk = 'x'` },
      ],
    }))

    expect(result.Responses).toBeDefined()
    expect(result.Responses!.length).toBe(2)

    // One response should succeed, the other should have an Error
    const errors = result.Responses!.filter(r => r.Error)
    const successes = result.Responses!.filter(r => !r.Error)
    expect(errors.length).toBe(1)
    expect(successes.length).toBe(1)
    expect(errors[0].Error!.Code).toBe('ResourceNotFound')
  })

  it('rejects an empty Statements array', async () => {
    await expectDynamoError(
      () => ddb.send(new BatchExecuteStatementCommand({
        Statements: [],
      })),
      'ValidationException',
    )
  })
})
