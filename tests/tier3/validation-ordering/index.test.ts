import {
  QueryCommand,
  ScanCommand,
  UpdateTableCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

// The non-existent-index error surface. Real AWS does NOT return
// IndexNotFoundException here (characterised in the U2 gap map): Query/Scan
// report ValidationException, and an UpdateTable GSI delete reports
// ResourceNotFoundException. A too-lenient target that invents its own code or
// silently succeeds fails these.

describe('Non-existent index — error surface', { tags: ['query', 'scan', 'data-plane'] }, () => {
  it('Query on a non-existent index reports ValidationException', async () => {
    try {
      await ddb.send(
        new QueryCommand({
          TableName: hashTableDef.name,
          IndexName: 'does_not_exist',
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: { '#pk': 'pk' },
          ExpressionAttributeValues: { ':pk': { S: 'x' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('does not have the specified index')
    }
  })

  it('Scan on a non-existent index reports ValidationException', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          IndexName: 'does_not_exist',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('does not have the specified index')
    }
  })

  it('UpdateTable deleting a non-existent GSI reports ResourceNotFoundException', async () => {
    try {
      await ddb.send(
        new UpdateTableCommand({
          TableName: hashTableDef.name,
          GlobalSecondaryIndexUpdates: [{ Delete: { IndexName: 'does_not_exist' } }],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ResourceNotFoundException')
      expect(err.message).toContain('Requested resource not found')
    }
  })
})
