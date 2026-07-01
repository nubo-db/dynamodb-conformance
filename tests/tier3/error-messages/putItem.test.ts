import {
  PutItemCommand,
  GetItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  hashBTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-put-null-false' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

// no negative-path: acceptance-mixed (asserts accepted and rejected cases)
describe('PutItem — exact error messages', { tags: ['put-item', 'data-plane'] }, () => {
  it('missing table name: full validation error string', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: undefined as unknown as string,
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value null at 'tableName' failed to satisfy constraint: Member must not be null",
      )
    }
  })

  it('empty table name: minimum length 1 error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: '',
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      // Structural assertion: pin the contractual field and constraint, float
      // the envelope prefix, echoed value and field casing that AWS varies by
      // region (2026-06 four-region capture; same idea as createTable's
      // backend-variant handling). See CONTRIBUTING, "error-messages".
      expect((err as DynamoDBServiceException).message.toLowerCase()).toContain(
        "tablename",
      )
      expect((err as DynamoDBServiceException).message).toContain(
        'failed to satisfy constraint: Member must have length greater than or equal to 1',
      )
    }
  })

  it('table name too long (256 chars): maximum length 255 error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: 'a'.repeat(256),
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `1 validation error detected: Value '${'a'.repeat(256)}' at 'tableName' failed to satisfy constraint: Member must have length less than or equal to 255`,
      )
    }
  })

  it('table name with invalid chars: regex pattern error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: 'bad table!@#',
          Item: { pk: { S: 'test' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value 'bad table!@#' at 'tableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+",
      )
    }
  })

  it('empty string set: full parameter-values-invalid error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: [] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'One or more parameter values were invalid: An string set  may not be empty',
      )
    }
  })

  it('empty number set: full parameter-values-invalid error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { NS: [] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'One or more parameter values were invalid: An number set  may not be empty',
      )
    }
  })

  it('duplicate values in SS: full duplicates error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: ['a', 'a'] } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      // Float the collection rendering (["a", "a"] in newer regions, [a, a] in
      // older ones); pin the bespoke message either side of it.
      expect((err as DynamoDBServiceException).message).toContain(
        'One or more parameter values were invalid: Input collection',
      )
      expect((err as DynamoDBServiceException).message).toContain('contains duplicates')
    }
  })

  it('NULL attr with false is accepted and normalises to NULL true', async () => {
    // AWS behaviour change captured 2026-06-08 (eu-west-2): PutItem with a
    // { NULL: false } attribute is no longer rejected. The value is accepted
    // and normalises to { NULL: true } on read.
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-put-null-false' }, attr1: { NULL: false } },
      }),
    )
    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'em-put-null-false' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item).toEqual({
      pk: { S: 'em-put-null-false' },
      attr1: { NULL: true },
    })
  })

  it('mixing expression and non-expression: full conflict error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
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

  it('ExpressionAttributeValues without expression: full unused-EAV error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          ExpressionAttributeValues: { ':v': { S: 'unused' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'ExpressionAttributeValues can only be specified when using expressions',
      )
    }
  })

  it('redundant parentheses in ConditionExpression: full error string', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-put-redundant' } },
          ConditionExpression: '((attribute_not_exists(pk)))',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'Invalid ConditionExpression: The expression has redundant parentheses;',
      )
    }
  })

  it('contains() with duplicate path and operand: full distinct-operand error', async () => {
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-put-contains-dup' } },
          ConditionExpression: 'contains(#a, #a)',
          ExpressionAttributeNames: { '#a': 'data' },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'Invalid ConditionExpression: The first operand must be distinct from the remaining operands for this operator or function; operator: contains, first operand: [data]',
      )
    }
  })

  it('empty-binary item key value: full ValidationException message', async () => {
    // Real AWS rejects a zero-length binary key value with a top-level
    // ValidationException, the binary analogue of the empty-string key rejection.
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashBTableDef.name,
          Item: { pk: { B: new Uint8Array([]) } },
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
