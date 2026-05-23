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
          TableName: '_conformance_valid_table_name',
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
          TableName: '_conformance_valid_table_name',
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

  it('rejects a duplicate attribute in KeySchema', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: '_conformance_dupkey',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'pk', KeyType: 'RANGE' },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('Invalid KeySchema')
    }
  })

  it('rejects more than two KeySchema elements', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: '_conformance_threekey',
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'sk', AttributeType: 'S' },
            { AttributeName: 'tk', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'sk', KeyType: 'RANGE' },
            { AttributeName: 'tk', KeyType: 'RANGE' },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      // The full message echoes the KeySchema as a Java-style object dump
      // (SDK-version-coupled), so assert only the stable constraint phrase.
      expect(err.message).toContain('Member must have length less than or equal to 2')
    }
  })

  it('rejects an invalid BillingMode on its own', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: '_conformance_badbilling',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          // @ts-expect-error -- testing invalid BillingMode
          BillingMode: 'INVALID_MODE',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('billingMode')
      expect(err.message).toContain('[PROVISIONED, PAY_PER_REQUEST]')
    }
  })
})
