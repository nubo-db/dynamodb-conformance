import {
  BatchGetItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('BatchGetItem — exact error messages', () => {
  it('empty RequestItems: full required-parameter error', async () => {
    try {
      await ddb.send(new BatchGetItemCommand({ RequestItems: {} }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The requestItems parameter is required for BatchGetItem',
      )
    }
  })

  it('> 100 keys across all tables: interpolated full error', async () => {
    // Only variable part is the table name (we own it via uniqueTableName),
    // so we keep the exact-match rung and interpolate.
    const keys = Array.from({ length: 101 }, (_, i) => ({ pk: { S: `bg-${i}` } }))
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: { [hashTableDef.name]: { Keys: keys } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `1 validation error detected: Value at 'RequestItems.${hashTableDef.name}.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100`,
      )
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            '_conformance_does_not_exist_em_bg': {
              Keys: [{ pk: { S: 'test' } }],
            },
          },
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

  it('duplicate keys in one Keys array: full duplicate-keys error', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: {
              Keys: [
                { pk: { S: 'em-bg-dup' } },
                { pk: { S: 'em-bg-dup' } },
              ],
            },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Provided list of item keys contains duplicates',
      )
    }
  })
})
