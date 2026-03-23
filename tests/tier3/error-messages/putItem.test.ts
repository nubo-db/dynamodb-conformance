import { PutItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-put-null-false' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
})

describe('PutItem — exact error messages', () => {
  it('missing table name: "Member must not be null"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: undefined as unknown as string,
          Item: { pk: { S: 'test' } },
        }),
      ),
      'ValidationException',
      'Member must not be null',
    )
  })

  it('empty table name: validation error for minimum length', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: '',
          Item: { pk: { S: 'test' } },
        }),
      ),
      'ValidationException',
      /Member must have length greater than or equal to 1|Member must satisfy regular expression/,
    )
  })

  it('table name too long (256 chars): "Member must have length less than or equal to 255"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: 'a'.repeat(256),
          Item: { pk: { S: 'test' } },
        }),
      ),
      'ValidationException',
      'Member must have length less than or equal to 255',
    )
  })

  it('table name with invalid chars: "Member must satisfy regular expression pattern"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: 'bad table!@#',
          Item: { pk: { S: 'test' } },
        }),
      ),
      'ValidationException',
      /Member must satisfy regular expression pattern: \[a-zA-Z0-9_.\-\]\+/,
    )
  })

  it('empty string set: "An string set  may not be empty"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: [] } },
        }),
      ),
      'ValidationException',
      'An string set  may not be empty',
    )
  })

  it('empty number set: "An number set  may not be empty"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { NS: [] } },
        }),
      ),
      'ValidationException',
      'An number set  may not be empty',
    )
  })

  it('duplicate values in SS: "Input collection [x, x] contains duplicates"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: ['a', 'a'] } },
        }),
      ),
      'ValidationException',
      /Input collection.*contains duplicates/,
    )
  })

  it('NULL attr with false: "Null attribute value types must have the value of true"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'em-put-null-false' }, attr1: { NULL: false } },
        }),
      ),
      'ValidationException',
      'Null attribute value types must have the value of true',
    )
  })

  it('mixing expression and non-expression: exact message', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          Expected: { pk: { Exists: false } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      ),
      'ValidationException',
      'Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}',
    )
  })

  it('ExpressionAttributeValues without expression: error message', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          ExpressionAttributeValues: { ':v': { S: 'unused' } },
        }),
      ),
      'ValidationException',
      'ExpressionAttributeValues can only be specified when using expressions',
    )
  })
})
