import {
  UpdateItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const hashKeys = [
  { pk: { S: 'em-upd-key-mod' } },
  { pk: { S: 'em-upd-type-mismatch' } },
]

const compositeKeys = [
  { pk: { S: 'em-upd-range-mod' }, sk: { S: 'sk1' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, hashKeys)
  await cleanupItems(compositeTableDef.name, compositeKeys)
})

describe('UpdateItem — exact error messages', () => {
  it('cannot update hash key attribute', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'new-val' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: Cannot update attribute pk. This attribute is part of the key',
      )
    }
  })

  it('invalid UpdateExpression syntax', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'INVALID SYNTAX HERE',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid UpdateExpression: Syntax error; token: "INVALID", near: "INVALID SYNTAX"',
      )
    }
  })

  it('unused ExpressionAttributeNames', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
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

  it('unused ExpressionAttributeValues', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' }, ':unused': { S: 'extra' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Value provided in ExpressionAttributeValues unused in expressions: keys: {:unused}',
      )
    }
  })

  it('missing ExpressionAttributeValues reference', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid UpdateExpression: An expression attribute value used in expression is not defined; attribute value: :v',
      )
    }
  })

  it('mixing UpdateExpression with AttributeUpdates', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          AttributeUpdates: {
            attr1: { Value: { S: 'val' }, Action: 'PUT' },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributeUpdates} Expression parameters: {UpdateExpression}',
      )
    }
  })

  it('empty UpdateExpression', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: '',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid UpdateExpression: The expression can not be empty;',
      )
    }
  })

  it('cannot update range key attribute on composite table', async () => {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: compositeTableDef.name,
          Key: { pk: { S: 'em-upd-range-mod' }, sk: { S: 'sk1' } },
          UpdateExpression: 'SET sk = :v',
          ExpressionAttributeValues: { ':v': { S: 'new-sk' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values were invalid: Cannot update attribute sk. This attribute is part of the key',
      )
    }
  })
})
