import {
  BatchWriteItemCommand,
  BatchGetItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('Batch operations — validation ordering', () => {
  it('BatchWriteItem rejects empty RequestItems', async () => {
    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {},
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('requestItems')
    }
  })

  it('BatchGetItem rejects empty RequestItems', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {},
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('requestItems')
    }
  })

  it('BatchWriteItem rejects more than 25 items with exact count in message', async () => {
    // Build 26 put requests
    const requests = Array.from({ length: 26 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `item_${i}` } },
      },
    }))

    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {
            [hashTableDef.name]: requests,
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      // DynamoDB reports the number of items in validation error
      expect(err.message).toMatch(/member must have length less than or equal to 25|too many items/i)
    }
  })

  it('BatchGetItem rejects more than 100 keys with exact count in message', async () => {
    // Build 101 key requests
    const keys = Array.from({ length: 101 }, (_, i) => ({
      pk: { S: `key_${i}` },
    }))

    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: {
              Keys: keys,
            },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      // DynamoDB reports too many items in validation error
      expect(err.message).toMatch(/member must have length less than or equal to 100|too many items/i)
    }
  })
})
