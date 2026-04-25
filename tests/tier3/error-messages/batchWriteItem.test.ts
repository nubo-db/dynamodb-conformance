import {
  BatchWriteItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-bw-dup' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

describe('BatchWriteItem — exact error messages', () => {
  it('empty RequestItems: full required-parameter error', async () => {
    try {
      await ddb.send(new BatchWriteItemCommand({ RequestItems: {} }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'The requestItems parameter is required for BatchWriteItem',
      )
    }
  })

  it('> 25 requests: anchored regex on the constraint phrase', async () => {
    // AWS echoes the entire WriteRequest list into the validation message
    // in a Java-toString format that includes every AttributeValue field
    // (recently they added `workloadProfileName`). Pinning the echo verbatim
    // with .toBe() would couple this assertion to the SDK's serialisation,
    // which has changed before and will again. Anchored regex around the
    // structural envelope (`{<table>=[<dump>]}`) and the constraint phrase
    // at the end lets the dump vary without weakening what we actually
    // care about: that the right validation fires.
    const requests = Array.from({ length: 26 }, (_, i) => ({
      PutRequest: { Item: { pk: { S: `bw-${i}` } } },
    }))
    const escapedName = hashTableDef.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const expectedPattern = new RegExp(
      `^1 validation error detected: Value '\\{${escapedName}=\\[.+\\]\\}' at 'requestItems' failed to satisfy constraint: Map value must satisfy constraint: \\[Member must have length less than or equal to 25, Member must have length greater than or equal to 1\\]$`,
      's',
    )
    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: { [hashTableDef.name]: requests },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toMatch(expectedPattern)
    }
  })

  it('same key Put and Delete in one batch: full duplicate-keys error', async () => {
    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {
            [hashTableDef.name]: [
              { PutRequest: { Item: { pk: { S: 'em-bw-dup' } } } },
              { DeleteRequest: { Key: { pk: { S: 'em-bw-dup' } } } },
            ],
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

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {
            '_conformance_does_not_exist_em_bw': [
              { PutRequest: { Item: { pk: { S: 'test' } } } },
            ],
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
})
