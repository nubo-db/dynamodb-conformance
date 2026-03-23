import {
  ScanCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('Scan — validation ordering', () => {
  it('rejects Segment without TotalSegments', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Segment: 0,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('Segment')
      expect(err.message).toContain('TotalSegments')
    }
  })

  it('rejects Segment >= TotalSegments', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Segment: 5,
          TotalSegments: 5,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('Segment')
      expect(err.message).toContain('TotalSegments')
    }
  })

  it('rejects TotalSegments without Segment', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          TotalSegments: 4,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      const err = e as DynamoDBServiceException
      expect(err.name).toBe('ValidationException')
      expect(err.message).toContain('Segment')
      expect(err.message).toContain('TotalSegments')
    }
  })
})
