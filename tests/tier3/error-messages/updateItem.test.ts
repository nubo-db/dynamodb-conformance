import { UpdateItemCommand, PutItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const hashKeys = [
  { pk: { S: 'em-upd-key-mod' } },
  { pk: { S: 'em-upd-type-mismatch' } },
]

const compositeKeys = [
  { pk: { S: 'em-upd-range-mod' }, sk: { S: 'sk1' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, hashKeys)
  await cleanupItems(compositeTableDef.name, compositeKeys)
})

describe('UpdateItem — exact error messages', () => {
  it('cannot update hash key attribute', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'new-val' } },
        }),
      ),
      'ValidationException',
      /Cannot update attribute.*key/i,
    )
  })

  it('invalid UpdateExpression syntax', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'INVALID SYNTAX HERE',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      ),
      'ValidationException',
      'Invalid UpdateExpression',
    )
  })

  it('unused ExpressionAttributeNames', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          ExpressionAttributeNames: { '#unused': 'someattr' },
        }),
      ),
      'ValidationException',
      'Value provided in ExpressionAttributeNames unused in expressions',
    )
  })

  it('unused ExpressionAttributeValues', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' }, ':unused': { S: 'extra' } },
        }),
      ),
      'ValidationException',
      'Value provided in ExpressionAttributeValues unused in expressions',
    )
  })

  it('missing ExpressionAttributeValues reference', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
        }),
      ),
      'ValidationException',
      /was not substituted|not defined|expression attribute value|Value provided in ExpressionAttributeValues unused in expressions|ExpressionAttributeValues must not be empty/,
    )
  })

  it('mixing UpdateExpression with AttributeUpdates', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: 'SET attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          AttributeUpdates: {
            attr1: { Value: { S: 'val' }, Action: 'PUT' },
          },
        }),
      ),
      'ValidationException',
      'Can not use both expression and non-expression',
    )
  })

  it('empty UpdateExpression', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'em-upd-key-mod' } },
          UpdateExpression: '',
        }),
      ),
      'ValidationException',
      'Invalid UpdateExpression',
    )
  })

  it('cannot update range key attribute on composite table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: compositeTableDef.name,
          Key: { pk: { S: 'em-upd-range-mod' }, sk: { S: 'sk1' } },
          UpdateExpression: 'SET sk = :v',
          ExpressionAttributeValues: { ':v': { S: 'new-sk' } },
        }),
      ),
      'ValidationException',
      /Cannot update attribute.*key/i,
    )
  })
})
