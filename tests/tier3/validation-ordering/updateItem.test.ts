import {
  UpdateItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'

describe('UpdateItem — validation ordering', () => {
  it('empty TableName reports only tableName constraint', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
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

  it('rejects invalid ReturnValues (UpdateItem reports the first enum error)', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
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
      // Behaviour change captured 2026-06-08: UpdateItem in eu-west-2 stops at
      // the first invalid enum (ReturnValues) and reports one error, where
      // older regions aggregated both. Pin the ground-truth single-error form.
      expect(err.message).toMatch(/^1 validation error detected:/)
      expect(err.message).toContain('enum value set')
    }
  })

  it('invalid table name pattern reports only tableName', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
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
