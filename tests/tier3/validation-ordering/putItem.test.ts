import {
  PutItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'

describe('PutItem — validation ordering', () => {
  it('empty TableName reports tableName constraint (stops early)', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: '',
          Item: {},
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('tableName')
    }
  })

  it('empty TableName with invalid ReturnValues reports only tableName', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: '',
          ReturnValues: 'INVALID',
          Item: {},
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('tableName')
    }
  })

  it('reports invalid ReturnConsumedCapacity, ReturnItemCollectionMetrics, and ReturnValues together', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: '_conformance_valid_table_name',
          Item: { pk: { S: 'test' } },
          ReturnConsumedCapacity: 'INVALID',
          ReturnItemCollectionMetrics: 'INVALID',
          ReturnValues: 'INVALID',
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('returnConsumedCapacity')
      expect(err.message).toContain('returnItemCollectionMetrics')
      expect(err.message).toContain('returnValues')
    }
  })

  it('reports invalid table name pattern', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: 'x!',
          Item: {},
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('tableName')
    }
  })
})
