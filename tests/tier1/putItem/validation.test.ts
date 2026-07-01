import { PutItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('PutItem — validation', { tags: ['put-item', 'data-plane', 'negative-path'] }, () => {
  it('rejects PutItem to a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: '_conformance_nonexistent_table',
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

  it('rejects duplicate values in number set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'test' }, bad: { NS: ['1', '1', '2'] } },
        }),
      ),
      'ValidationException',
      'duplicates',
    )
  })

  it('rejects duplicate values in binary set', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: {
            pk: { S: 'test' },
            bad: { BS: [new Uint8Array([1]), new Uint8Array([1])] },
          },
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

  // An attribute used as a table or index key must match its declared scalar
  // type and may not be empty. lsi1sk is declared S and is an index key on the
  // composite table, so these writes are rejected even though the base-table
  // keys (pk, sk) are valid.
  it('rejects a wrong-typed index key attribute', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: { pk: { S: 'idxkey-type' }, sk: { S: 'a' }, lsi1sk: { N: '5' } },
        }),
      ),
      'ValidationException',
      /Type mismatch for Index Key/,
    )
  })

  it('rejects a non-scalar index key attribute', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: {
            pk: { S: 'idxkey-nonscalar' },
            sk: { S: 'b' },
            lsi1sk: { L: [{ S: 'x' }] },
          },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects an empty-string index key attribute', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: { pk: { S: 'idxkey-empty' }, sk: { S: 'c' }, lsi1sk: { S: '' } },
        }),
      ),
      'ValidationException',
      /empty string/i,
    )
  })

  it('rejects a number with a leading space', async () => {
    await expectDynamoError(
      () => ddb.send(new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: { S: 'num-lead-ws' }, n: { N: ' 5' } } })),
      'ValidationException',
    )
  })

  it('rejects a number with a trailing space', async () => {
    await expectDynamoError(
      () => ddb.send(new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: { S: 'num-trail-ws' }, n: { N: '5 ' } } })),
      'ValidationException',
    )
  })

})
