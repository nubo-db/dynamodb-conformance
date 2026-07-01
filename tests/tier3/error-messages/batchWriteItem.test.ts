import {
  BatchWriteItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  hashBTableDef,
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-bw-dup' } },
]

// Defensive: the invalid-index-key cases below fail validation and write
// nothing; clean up anyway in case a too-lenient target persists them.
const compositeKeysToCleanup = [
  { pk: { S: 'em-bw-idx-type' }, sk: { S: 'a' } },
  { pk: { S: 'em-bw-idx-nonscalar' }, sk: { S: 'b' } },
  { pk: { S: 'em-bw-idx-empty' }, sk: { S: 'c' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
  await cleanupItems(compositeTableDef.name, compositeKeysToCleanup)
})

describe('BatchWriteItem — exact error messages', { tags: ['batch', 'data-plane', 'negative-path'] }, () => {
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

  // Invalid key value inside a PutRequest item. BatchWriteItem validates up front,
  // so every variant is a top-level ValidationException (no cancellation path).
  // Strings captured from real AWS, invariant across four regions (eu-west-2,
  // us-east-1, ap-southeast-2, eu-central-1; 2026-06-23) — so the table-key
  // schema-mismatch message is the contract, not a region-local wording.
  // Index-key messages name gsi1, the alphabetically-first index lsi1sk keys on
  // compositeTableDef.
  const expectExactValidation = async (
    command: BatchWriteItemCommand,
    message: string,
  ) => {
    try {
      await ddb.send(command)
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(message)
    }
  }

  it('wrong-typed table key: full schema-mismatch message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: [{ PutRequest: { Item: { pk: { N: '5' } } } }] },
      }),
      'The provided key element does not match the schema',
    )
  })

  it('non-scalar table key: full schema-mismatch message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: [{ PutRequest: { Item: { pk: { L: [{ S: 'x' }] } } } }] },
      }),
      'The provided key element does not match the schema',
    )
  })

  it('empty-string table key: full empty-value message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: [{ PutRequest: { Item: { pk: { S: '' } } } }] },
      }),
      'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk',
    )
  })

  it('empty-binary put-request key: full empty-value message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: { [hashBTableDef.name]: [{ PutRequest: { Item: { pk: { B: new Uint8Array([]) } } } }] },
      }),
      'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk',
    )
  })

  it('empty-binary delete-request key: full empty-value message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: { [hashBTableDef.name]: [{ DeleteRequest: { Key: { pk: { B: new Uint8Array([]) } } } }] },
      }),
      'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk',
    )
  })

  it('wrong-typed index key: full type-mismatch message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: {
          [compositeTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'em-bw-idx-type' }, sk: { S: 'a' }, lsi1sk: { N: '5' } } } },
          ],
        },
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: N IndexName: gsi1',
    )
  })

  it('non-scalar index key: full type-mismatch message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: {
          [compositeTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'em-bw-idx-nonscalar' }, sk: { S: 'b' }, lsi1sk: { L: [{ S: 'x' }] } } } },
          ],
        },
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: L IndexName: gsi1',
    )
  })

  it('empty-string index key: full secondary-index-key message', async () => {
    await expectExactValidation(
      new BatchWriteItemCommand({
        RequestItems: {
          [compositeTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'em-bw-idx-empty' }, sk: { S: 'c' }, lsi1sk: { S: '' } } } },
          ],
        },
      }),
      'One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: gsi1, IndexKey: lsi1sk',
    )
  })
})
