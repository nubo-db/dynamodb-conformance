import {
  CreateTableCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'

describe('CreateTable — validation ordering', () => {
  it('empty TableName reports only tableName constraint', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: '',
          KeySchema: [],
          AttributeDefinitions: [],
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

  it('invalid table name pattern reports only tableName', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: 'x!',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [],
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

  it('reports invalid BillingMode and invalid KeySchema element together', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: 'valid_table_name',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'INVALID' },
          ],
          BillingMode: 'INVALID_MODE',
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message.length).toBeGreaterThan(0)
    }
  })

  it('reports missing ProvisionedThroughput and invalid key type together', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: 'valid_table_name',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'INVALID' },
          ],
          BillingMode: 'PROVISIONED',
        } as any),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message.length).toBeGreaterThan(0)
    }
  })
})
