import { PutItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('PutItem — validation', () => {
  it('rejects PutItem to a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: 'this_table_does_not_exist_xyz',
          Item: { pk: { S: 'test' } },
        }),
      ),
      'ResourceNotFoundException',
    )
  })

  it('rejects item missing the hash key', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { notTheKey: { S: 'test' } },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects empty string set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: [] } },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects empty number set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { NS: [] } },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects empty binary set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { BS: [] } },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects duplicate values in string set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { SS: ['a', 'a'] } },
        }),
      ),
      'ValidationException',
      'duplicates',
    )
  })

  it('rejects invalid ReturnValues', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' } },
          // @ts-expect-error -- testing invalid ReturnValues
          ReturnValues: 'INVALID',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects mixing expression and non-expression parameters', async () => {
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
      'Can not use both expression and non-expression',
    )
  })
})
