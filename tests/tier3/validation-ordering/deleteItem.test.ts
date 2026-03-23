import {
  DeleteItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'

describe('DeleteItem — validation ordering', () => {
  it('empty TableName reports only tableName constraint', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: '',
          Key: {},
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

  it('reports invalid ReturnValues and invalid ReturnConsumedCapacity together', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: 'valid_table_name',
          Key: { pk: { S: 'test' } },
          ReturnValues: 'INVALID',
          ReturnConsumedCapacity: 'INVALID',
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('returnValues')
      expect(err.message).toContain('returnConsumedCapacity')
    }
  })

  it('invalid table name pattern reports only tableName', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: 'x!',
          Key: {},
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
