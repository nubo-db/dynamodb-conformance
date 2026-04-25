import {
  ScanCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('Scan — exact error messages', () => {
  it('Segment without TotalSegments: full required-parameter error', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Segment: 0,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The TotalSegments parameter is required but was not present in the request when Segment parameter is present',
      )
    }
  })

  it('Segment >= TotalSegments: full out-of-range error', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Segment: 5,
          TotalSegments: 5,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The Segment parameter is zero-based and must be less than parameter TotalSegments: Segment: 5 is not less than TotalSegments: 5',
      )
    }
  })

  it('TotalSegments without Segment: full required-parameter error', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          TotalSegments: 4,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The Segment parameter is required but was not present in the request when parameter TotalSegments is present',
      )
    }
  })

  it('Limit of 0: full minimum-value error', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Limit: 0,
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value '0' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1",
      )
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new ScanCommand({
          TableName: '_conformance_does_not_exist_em_scan',
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
})
