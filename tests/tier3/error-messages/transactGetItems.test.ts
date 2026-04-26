import {
  TransactGetItemsCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('TransactGetItems — exact error messages', () => {
  it('empty TransactItems: full minimum-length error', async () => {
    try {
      await ddb.send(new TransactGetItemsCommand({ TransactItems: [] }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
      )
    }
  })

  it('> 100 gets: anchored regex on the constraint phrase', async () => {
    // Same Java-toString dump shape as TransactWriteItems / BatchWriteItem;
    // anchor around the dump rather than pinning it verbatim.
    const items = Array.from({ length: 101 }, (_, i) => ({
      Get: {
        TableName: hashTableDef.name,
        Key: { pk: { S: `tgi-${i}` } },
      },
    }))
    const expectedPattern = new RegExp(
      `^1 validation error detected: Value '\\[.+\\]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100$`,
      's',
    )
    try {
      await ddb.send(new TransactGetItemsCommand({ TransactItems: items }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toMatch(expectedPattern)
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new TransactGetItemsCommand({
          TransactItems: [
            {
              Get: {
                TableName: '_conformance_does_not_exist_em_tgi',
                Key: { pk: { S: 'x' } },
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

  it('invalid ProjectionExpression syntax: full parser error', async () => {
    // Request-level ValidationException — the parser rejects the expression
    // before any per-action processing runs.
    try {
      await ddb.send(
        new TransactGetItemsCommand({
          TransactItems: [
            {
              Get: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tgi-pe' } },
                ProjectionExpression: '!!!',
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
        'Invalid ProjectionExpression: Syntax error; token: "!", near: "!!"',
      )
    }
  })

  it('missing key attribute: action-level ValidationError surfaces as TransactionCanceledException', async () => {
    // Per-action validation (here: an empty Key) is reported through the
    // cancellation channel rather than as a request-level ValidationException.
    // The reason code is 'ValidationError', not 'ConditionalCheckFailed'.
    try {
      await ddb.send(
        new TransactGetItemsCommand({
          TransactItems: [
            {
              Get: {
                TableName: hashTableDef.name,
                Key: {},
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      const expectedReasons = ['ValidationError'] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
    }
  })
})
