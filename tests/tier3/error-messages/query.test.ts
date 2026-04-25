import {
  QueryCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  compositeTableDef,
} from '../../../src/helpers.js'

describe('Query — exact error messages', () => {
  it('missing hash key in KeyConditionExpression', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'sk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Query condition missed key schema element: pk',
      )
    }
  })

  it('non-key attribute in KeyConditionExpression', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Query condition missed key schema element: pk',
      )
    }
  })

  it('unused ExpressionAttributeNames', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          ExpressionAttributeNames: { '#unused': 'someattr' },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Value provided in ExpressionAttributeNames unused in expressions: keys: {#unused}',
      )
    }
  })

  it('invalid Select value', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          // @ts-expect-error -- testing invalid Select
          Select: 'INVALID_VALUE',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value 'INVALID_VALUE' at 'select' failed to satisfy constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]",
      )
    }
  })

  it('ConsistentRead on GSI', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          IndexName: 'gsi1',
          KeyConditionExpression: '#hk = :v',
          ExpressionAttributeNames: { '#hk': 'lsi1sk' },
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          ConsistentRead: true,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Consistent reads are not supported on global secondary indexes',
      )
    }
  })

  it('Limit of 0', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          Limit: 0,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
      )
    }
  })

  it('empty KeyConditionExpression', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: '',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid KeyConditionExpression: The expression can not be empty;',
      )
    }
  })

  it('filter references undefined ExpressionAttributeNames', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :pk',
          FilterExpression: '#missing = :fv',
          ExpressionAttributeValues: { ':pk': { S: 'val' }, ':fv': { S: 'x' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #missing',
      )
    }
  })
})
