import {
  ExecuteTransactionCommand,
  ExecuteStatementCommand,
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('ExecuteTransaction — PartiQL', { tags: ['partiql', 'data-plane'] }, () => {
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

  it('transactional INSERT and UPDATE both succeed atomically', async () => {
    keysToCleanup.push(
      { pk: { S: 'txn-insert-1' } },
      { pk: { S: 'txn-update-1' } },
    )

    // Seed an item for the UPDATE
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'txn-update-1' }, data: { S: 'before' } },
    }))

    await ddb.send(new ExecuteTransactionCommand({
      TransactStatements: [
        { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-insert-1', 'data': 'txn-new'}` },
        { Statement: `UPDATE "${hashTableDef.name}" SET data = 'txn-after' WHERE pk = 'txn-update-1'` },
      ],
    }))

    const inserted = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'txn-insert-1' } },
      ConsistentRead: true,
    }))
    expect(inserted.Item).toBeDefined()
    expect(inserted.Item!.data.S).toBe('txn-new')

    const updated = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'txn-update-1' } },
      ConsistentRead: true,
    }))
    expect(updated.Item).toBeDefined()
    expect(updated.Item!.data.S).toBe('txn-after')
  })

  it('transaction rolls back on duplicate key INSERT', async () => {
    keysToCleanup.push(
      { pk: { S: 'txn-dup-1' } },
      { pk: { S: 'txn-dup-2' } },
    )

    // Seed an item that will cause the duplicate conflict
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'txn-dup-1' }, data: { S: 'existing' } },
    }))

    // Transaction: INSERT duplicate key + INSERT new key
    // The duplicate INSERT should cause the entire transaction to fail
    await expectDynamoError(
      () => ddb.send(new ExecuteTransactionCommand({
        TransactStatements: [
          { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-dup-1', 'data': 'should-fail'}` },
          { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-dup-2', 'data': 'should-rollback'}` },
        ],
      })),
      'TransactionCanceledException',
    )

    // Verify rollback: txn-dup-2 should not exist
    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'txn-dup-2' } },
      ConsistentRead: true,
    }))
    expect(result.Item).toBeUndefined()

    // Original item should be unchanged
    const original = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'txn-dup-1' } },
      ConsistentRead: true,
    }))
    expect(original.Item!.data.S).toBe('existing')
  })

  it('multiple INSERTs in one transaction', async () => {
    keysToCleanup.push(
      { pk: { S: 'txn-multi-1' } },
      { pk: { S: 'txn-multi-2' } },
      { pk: { S: 'txn-multi-3' } },
    )

    await ddb.send(new ExecuteTransactionCommand({
      TransactStatements: [
        { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-multi-1', 'data': 'a'}` },
        { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-multi-2', 'data': 'b'}` },
        { Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'txn-multi-3', 'data': 'c'}` },
      ],
    }))

    for (const [key, val] of [['txn-multi-1', 'a'], ['txn-multi-2', 'b'], ['txn-multi-3', 'c']]) {
      const result = await ddb.send(new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: key } },
        ConsistentRead: true,
      }))
      expect(result.Item).toBeDefined()
      expect(result.Item!.data.S).toBe(val)
    }
  })

  // ExecuteTransaction takes a ClientRequestToken, like TransactWriteItems. A
  // same-token replay inside the idempotency window returns the stored result
  // rather than re-running the statements, so the write applies exactly once.
  // Capacity accounting mirrors TransactWriteItems: the first call reports the
  // transactional write, the replay a transactional read of the stored result.
  // Values characterised against real DynamoDB (eu-west-2). See #70.
  it('idempotent replay under the same ClientRequestToken does not double-apply', async () => {
    const pk = `txn-idem-${Date.now()}`
    keysToCleanup.push({ pk: { S: pk } })

    // Seed a counter at 0, then increment it by 1 inside a tokenised transaction.
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': '${pk}', 'n': 0}`,
    }))
    const token = `et-idem-token-${Date.now()}`
    const statements = [
      { Statement: `UPDATE "${hashTableDef.name}" SET n = n + 1 WHERE pk = '${pk}'` },
    ]

    const first = await ddb.send(new ExecuteTransactionCommand({
      ClientRequestToken: token,
      ReturnConsumedCapacity: 'INDEXES',
      TransactStatements: statements,
    }))
    const replay = await ddb.send(new ExecuteTransactionCommand({
      ClientRequestToken: token,
      ReturnConsumedCapacity: 'INDEXES',
      TransactStatements: statements,
    }))

    // The increment applied exactly once — a replay, not a second execution.
    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: pk } },
      ConsistentRead: true,
    }))
    expect(after.Item!.n.N).toBe('1')

    // ExecuteTransaction reports ConsumedCapacity as a per-table array. The first
    // call is a transactional write (2 WCU, no read); the replay is a
    // transactional read of the stored result (2 RCU, no write).
    const firstEntry = (first.ConsumedCapacity ?? [])[0]
    const replayEntry = (replay.ConsumedCapacity ?? [])[0]
    expect(firstEntry?.WriteCapacityUnits).toBe(2)
    expect(firstEntry?.ReadCapacityUnits).toBeUndefined()
    expect(replayEntry?.ReadCapacityUnits).toBe(2)
    expect(replayEntry?.WriteCapacityUnits).toBeUndefined()
  })

  it('rejects empty TransactStatements', async () => {
    await expectDynamoError(
      () => ddb.send(new ExecuteTransactionCommand({
        TransactStatements: [],
      })),
      'ValidationException',
    )
  })
})
