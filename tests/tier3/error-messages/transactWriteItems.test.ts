import {
  TransactWriteItemsCommand,
  PutItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-twi-dup' } },
  { pk: { S: 'em-twi-multi-1' } },
  { pk: { S: 'em-twi-multi-2' } },
  { pk: { S: 'em-twi-pos' } },
  { pk: { S: 'em-twi-pos-new' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

describe('TransactWriteItems — exact error messages', () => {
  it('empty TransactItems: full minimum-length error', async () => {
    try {
      await ddb.send(new TransactWriteItemsCommand({ TransactItems: [] }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
      )
    }
  })

  it('> 100 actions: anchored regex on the constraint phrase', async () => {
    // AWS echoes the entire TransactItems list into the validation message in
    // the same Java-toString shape used for BatchWriteItem. Pinning the dump
    // verbatim with .toBe() couples the assertion to SDK serialisation, which
    // has changed before. Anchored regex around the structural envelope
    // (`[<dump>]`) and the constraint phrase at the end lets the dump vary
    // without weakening what we actually care about.
    const items = Array.from({ length: 101 }, (_, i) => ({
      Put: {
        TableName: hashTableDef.name,
        Item: { pk: { S: `twi-${i}` } },
      },
    }))
    const expectedPattern = new RegExp(
      `^1 validation error detected: Value '\\[.+\\]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100$`,
      's',
    )
    try {
      await ddb.send(new TransactWriteItemsCommand({ TransactItems: items }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toMatch(expectedPattern)
    }
  })

  it('duplicate target keys in same transaction: full multi-op error', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-dup' } },
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-dup' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Transaction request cannot include multiple operations on one item',
      )
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: '_conformance_does_not_exist_em_twi',
                Item: { pk: { S: 'x' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ResourceNotFoundException)
      expect((err as ResourceNotFoundException).name).toBe(
        'ResourceNotFoundException',
      )
      expect((err as ResourceNotFoundException).message).toBe(
        'Requested resource not found',
      )
    }
  })

  it('two failing actions: multi-reason TransactionCanceledException', async () => {
    // Integration: both actions in the transaction violate their conditions.
    // The cancellation summary lists every action's reason code in order, so
    // we can build the expected message from the same array we cross-check
    // against `CancellationReasons[].Code` — fully deterministic, no regex.
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-multi-1' }, attr1: { S: 'exists' } },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-multi-2' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-multi-1' }, attr1: { S: 'over' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-multi-2' }, attr1: { S: 'over' } },
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
      const expectedReasons = [
        'ConditionalCheckFailed',
        'ConditionalCheckFailed',
      ] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
    }
  })

  it('one passing, one failing: positional reason codes (None for the survivor)', async () => {
    // The cancellation summary reports a code per action *positionally* —
    // 'None' for actions that would have succeeded, the actual failure code
    // for actions that did not. Pin the full message and structure so the
    // suite catches emulators that emit a flat 'ConditionalCheckFailed' list
    // without per-action accounting.
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-pos' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-pos-new' }, attr1: { S: 'fresh' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-pos' }, attr1: { S: 'over' } },
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
      const expectedReasons = ['None', 'ConditionalCheckFailed'] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
      expect(txErr.CancellationReasons?.[1].Message).toBe(
        'The conditional request failed',
      )
    }
  })
})
