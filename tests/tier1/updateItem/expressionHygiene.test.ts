import {
  UpdateItemCommand,
  DynamoDBServiceException,
  type UpdateItemCommandInput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('UpdateItem — expression attribute hygiene', { tags: ['update-item', 'data-plane', 'negative-path'] }, () => {
  const key = { pk: { S: 'expr-hygiene' } }
  const expectRejected = async (extra: Partial<UpdateItemCommandInput>) => {
    try {
      await ddb.send(new UpdateItemCommand({ TableName: hashTableDef.name, Key: key, ...extra }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  }

  it('rejects an ExpressionAttributeNames key without a # prefix', () =>
    expectRejected({
      UpdateExpression: 'SET #s = :v',
      ExpressionAttributeNames: { s: 'status' },
      ExpressionAttributeValues: { ':v': { S: 'x' } },
    }))

  it('rejects an ExpressionAttributeValues key without a : prefix', () =>
    expectRejected({
      UpdateExpression: 'SET #s = :v',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: { v: { S: 'x' } },
    }))

  it('rejects ExpressionAttributeNames supplied with no expression', () =>
    expectRejected({
      ExpressionAttributeNames: { '#s': 'status' },
    }))

  it('rejects an unused ExpressionAttributeNames entry', () =>
    expectRejected({
      UpdateExpression: 'SET #s = :v',
      ExpressionAttributeNames: { '#s': 'status', '#unused': 'other' },
      ExpressionAttributeValues: { ':v': { S: 'x' } },
    }))
})
