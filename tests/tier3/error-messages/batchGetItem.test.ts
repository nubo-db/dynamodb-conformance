import {
  BatchGetItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, hashBTableDef, compositeTableDef } from '../../../src/helpers.js'

describe('BatchGetItem — exact error messages', { tags: ['batch', 'data-plane', 'negative-path'] }, () => {
  it('empty RequestItems: full required-parameter error', async () => {
    try {
      await ddb.send(new BatchGetItemCommand({ RequestItems: {} }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The requestItems parameter is required for BatchGetItem',
      )
    }
  })

  it('> 100 keys across all tables: interpolated full error', async () => {
    // Only variable part is the table name (we own it via uniqueTableName),
    // so we keep the exact-match rung and interpolate.
    const keys = Array.from({ length: 101 }, (_, i) => ({ pk: { S: `bg-${i}` } }))
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: { [hashTableDef.name]: { Keys: keys } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `1 validation error detected: Value at 'RequestItems.${hashTableDef.name}.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100`,
      )
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            '_conformance_does_not_exist_em_bg': {
              Keys: [{ pk: { S: 'test' } }],
            },
          },
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

  it('duplicate keys in one Keys array: full duplicate-keys error', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: {
              Keys: [
                { pk: { S: 'em-bg-dup' } },
                { pk: { S: 'em-bg-dup' } },
              ],
            },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Provided list of item keys contains duplicates',
      )
    }
  })

  // A KeysAndAttributes block accepts both the legacy AttributesToGet and a modern
  // ProjectionExpression; supplying both is the expression/non-expression conflict,
  // rejected before any read.
  it('AttributesToGet with ProjectionExpression in a KeysAndAttributes block: full conflict error', async () => {
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: {
              Keys: [{ pk: { S: 'em-bg-mix' } }],
              AttributesToGet: ['pk'],
              ProjectionExpression: 'pk',
            },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}',
      )
    }
  })

  it('empty-binary lookup key: full empty-value message', async () => {
    // Real AWS rejects a zero-length binary key value with a top-level
    // ValidationException, the binary analogue of the empty-string key rejection.
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashBTableDef.name]: { Keys: [{ pk: { B: new Uint8Array([]) } }] },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk',
      )
    }
  })

  it('mixing ProjectionExpression on one table and AttributesToGet on another is rejected', async () => {
    // Each table's block is internally consistent, but real AWS rejects the
    // request as a whole for mixing expression and non-expression projection
    // across tables.
    try {
      await ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: { Keys: [{ pk: { S: 'bg-mix-a' } }], ProjectionExpression: 'pk' },
            [compositeTableDef.name]: {
              Keys: [{ pk: { S: 'bg-mix-b' }, sk: { S: 'z' } }],
              AttributesToGet: ['pk'],
            },
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })
})
