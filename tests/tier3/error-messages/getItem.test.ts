import {
  GetItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
} from '../../../src/helpers.js'

describe('GetItem — exact error messages', () => {
  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new GetItemCommand({
          TableName: '_conformance_does_not_exist_em_get',
          Key: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ResourceNotFoundException)
      expect((err as ResourceNotFoundException).name).toBe(
        'ResourceNotFoundException',
      )
      expect((err as ResourceNotFoundException).message).toBe(
        'Requested resource not found',
      )
    }
  })

  it('malformed Key (missing range key on composite table): full schema-mismatch error', async () => {
    try {
      await ddb.send(
        new GetItemCommand({
          TableName: compositeTableDef.name,
          Key: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The provided key element does not match the schema',
      )
    }
  })

  it('invalid ProjectionExpression syntax: full parser error', async () => {
    try {
      await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'test' } },
          ProjectionExpression: '!!! INVALID !!!',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid ProjectionExpression: Syntax error; token: "!", near: "!!"',
      )
    }
  })
})
