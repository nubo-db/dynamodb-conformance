import {
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  TransactWriteItemsCommand,
  ConditionalCheckFailedException,
  TransactionCanceledException,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-ccf-put' } },
  { pk: { S: 'em-ccf-del' } },
  { pk: { S: 'em-ccf-upd' } },
  { pk: { S: 'em-ccf-txn' } },
  { pk: { S: 'em-ccf-rvocf' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

describe('Conditional check — exact error messages', () => {
  it('ConditionalCheckFailedException has message "The conditional request failed"', async () => {
    // Seed an item so the condition fails
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-ccf-put' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-ccf-put' }, attr1: { S: 'overwrite' } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConditionalCheckFailedException)
      expect((err as DynamoDBServiceException).message).toBe(
        'The conditional request failed',
      )
    }
  })

  it('ConditionalCheckFailed on PutItem with attribute_not_exists', async () => {
    // Item already seeded above
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-ccf-put' }, attr1: { S: 'again' } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConditionalCheckFailedException)
      expect((err as DynamoDBServiceException).name).toBe(
        'ConditionalCheckFailedException',
      )
    }
  })

  it('ConditionalCheckFailed on DeleteItem with condition', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-ccf-del' }, attr1: { S: 'keep' } },
      }),
    )

    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-ccf-del' } },
          ConditionExpression: 'attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'wrong-value' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConditionalCheckFailedException)
      expect((err as DynamoDBServiceException).message).toBe(
        'The conditional request failed',
      )
    }
  })

  it('ConditionalCheckFailed on UpdateItem with condition', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-ccf-upd' }, attr1: { S: 'original' } },
      }),
    )

    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-ccf-upd' } },
          UpdateExpression: 'SET attr1 = :newval',
          ConditionExpression: 'attr1 = :expected',
          ExpressionAttributeValues: {
            ':newval': { S: 'updated' },
            ':expected': { S: 'wrong' },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConditionalCheckFailedException)
      expect((err as DynamoDBServiceException).message).toBe(
        'The conditional request failed',
      )
    }
  })

  it('TransactionCanceledException message format', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-ccf-txn' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-ccf-txn' }, attr1: { S: 'overwrite' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      // Build the expected message and the structural cross-check from a
      // single source of truth — the reasons array we drive via the failing
      // ConditionExpression. AWS's cancellation-reasons summary is fully
      // deterministic given known inputs, so this is exact-match on both
      // the message and the parsed structure, not a regex tolerance.
      const expectedReasons = ['ConditionalCheckFailed'] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
    }
  })

  it('ReturnValuesOnConditionCheckFailure: ALL_OLD returns item in error', async () => {
    const originalItem = {
      pk: { S: 'em-ccf-rvocf' },
      attr1: { S: 'original-value' },
    }
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: originalItem,
      }),
    )

    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-ccf-rvocf' }, attr1: { S: 'overwrite' } },
          ConditionExpression: 'attribute_not_exists(pk)',
          ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ConditionalCheckFailedException)
      const ccfErr = err as ConditionalCheckFailedException
      expect(ccfErr.message).toBe('The conditional request failed')
      // The old item should be returned on the error
      expect((ccfErr as any).Item).toBeDefined()
      expect((ccfErr as any).Item.pk.S).toBe('em-ccf-rvocf')
      expect((ccfErr as any).Item.attr1.S).toBe('original-value')
    }
  })
})
