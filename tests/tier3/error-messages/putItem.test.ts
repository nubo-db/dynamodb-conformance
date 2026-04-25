import {
  PutItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-put-null-false' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

describe('PutItem — exact error messages', () => {
  it('missing table name: full validation error string', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: undefined as unknown as string,
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null",
      )
    }
  })

  it('empty table name: minimum length 1 error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: '',
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value '' at 'tableName' failed to satisfy constraint: Member must have length greater than or equal to 1",
      )
    }
  })

  it('table name too long (256 chars): maximum length 255 error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: 'a'.repeat(256),
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `1 validation error detected: Value '${'a'.repeat(256)}' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255`,
      )
    }
  })

  it('table name with invalid chars: regex pattern error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: 'bad table!@#',
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value 'bad table!@#' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
      )
    }
  })

  it('empty string set: full parameter-values-invalid error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: [] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: An string set  may not be empty',
      )
    }
  })

  it('empty number set: full parameter-values-invalid error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { NS: [] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: An number set  may not be empty',
      )
    }
  })

  it('duplicate values in SS: full duplicates error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: ['a', 'a'] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: Input collection [a, a] contains duplicates.',
      )
    }
  })

  it('NULL attr with false: full null-attribute error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-put-null-false' }, attr1: { NULL: false } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: Null attribute value types must have the value of true',
      )
    }
  })

  it('mixing expression and non-expression: full conflict error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          Expected: { pk: { Exists: false } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}',
      )
    }
  })

  it('ExpressionAttributeValues without expression: full unused-EAV error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          ExpressionAttributeValues: { ':v': { S: 'unused' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'ExpressionAttributeValues can only be specified when using expressions: ConditionExpression is null',
      )
    }
  })
})
