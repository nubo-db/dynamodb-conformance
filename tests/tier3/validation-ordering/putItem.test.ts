import {
  PutItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'

describe('PutItem — validation ordering', { tags: ['put-item', 'data-plane'] }, () => {
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
      // The field is named 'TableName' (eu-west-2, 2026-06) or 'tableName'
      // (older regions); match case-insensitively. Exactly one error proves
      // validation stops at the table name rather than also reporting the
      // empty Item.
      expect(err.message.toLowerCase()).toContain('tablename')
      expect(err.message).toMatch(/^1 validation error detected:/)
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
      // eu-west-2 (2026-06) stops at the table name and does not also report
      // the invalid ReturnValues; older regions aggregate both. Pin the
      // ground-truth short-circuit: table name present, ReturnValues absent.
      expect(err.message.toLowerCase()).toContain('tablename')
      expect(err.message.toLowerCase()).not.toContain('returnvalues')
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
      // All three invalid enums are caught (3 errors). eu-west-2 names
      // returnConsumedCapacity and ReturnItemCollectionMetrics explicitly and
      // reports the ReturnValues enum violation generically, so assert the
      // count plus the two named fields (case-insensitive across regions).
      expect(err.message).toMatch(/^3 validation errors detected:/)
      expect(err.message.toLowerCase()).toContain('returnconsumedcapacity')
      expect(err.message.toLowerCase()).toContain('returnitemcollectionmetrics')
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
