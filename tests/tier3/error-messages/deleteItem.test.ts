import {
  DeleteItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, hashTableDef, hashBTableDef } from '../../../src/helpers.js'

// Conditional-check failures for DeleteItem live in conditionalCheck.test.ts —
// that file owns the conditional-check error family across operations.

describe('DeleteItem — exact error messages', { tags: ['delete-item', 'data-plane', 'negative-path'] }, () => {
  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: '_conformance_does_not_exist_em_delete',
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
        new DeleteItemCommand({
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

  // Completes the expression/non-expression mutual-exclusion family for the item
  // writes (PutItem and UpdateItem already pin it). DeleteItem takes legacy Expected
  // and a modern ConditionExpression; supplying both is rejected up front.
  it('mixing Expected with ConditionExpression: full conflict error', async () => {
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-del-mix' } },
          Expected: { pk: { Exists: false } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}',
      )
    }
  })

  it('empty-binary key value: full ValidationException message', async () => {
    // Real AWS rejects a zero-length binary key value with a top-level
    // ValidationException, the binary analogue of the empty-string key rejection.
    try {
      await ddb.send(
        new DeleteItemCommand({
          TableName: hashBTableDef.name,
          Key: { pk: { B: new Uint8Array([]) } },
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
})
