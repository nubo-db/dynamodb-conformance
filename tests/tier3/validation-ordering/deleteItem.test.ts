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
      // Field is 'TableName' (eu-west-2, 2026-06) or 'tableName' (older
      // regions); match case-insensitively. One error proves it stops at the
      // table name rather than also reporting the empty Key.
      expect(err.message.toLowerCase()).toContain('tablename')
      expect(err.message).toMatch(/^1 validation error detected:/)
    }
  })

  it('reports invalid ReturnValues and invalid ReturnConsumedCapacity together', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: '_conformance_valid_table_name',
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
      // Both invalid enums are caught (2 errors). eu-west-2 names
      // returnConsumedCapacity and reports the ReturnValues enum violation
      // generically; assert the count plus the named field.
      expect(err.message).toMatch(/^2 validation errors detected:/)
      expect(err.message.toLowerCase()).toContain('returnconsumedcapacity')
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
