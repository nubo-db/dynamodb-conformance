import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError, cleanupItems } from '../../../src/helpers.js'

// Exercises the ConditionExpression parser with parenthesised forms.
// Emulator parsers are often not shared across expression contexts, so
// parens working in KeyConditionExpression does not imply they work here.
describe('PutItem — ConditionExpression parens', () => {
  const seeded = 'put-cep-seed'
  const freshKeys = [
    'put-cep-fresh-percond',
    'put-cep-fresh-fullwrap',
    'put-cep-fresh-nested',
  ]

  beforeAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      [seeded, ...freshKeys].map((k) => ({ pk: { S: k } })),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: seeded }, status: { S: 'active' } },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      [seeded, ...freshKeys].map((k) => ({ pk: { S: k } })),
    )
  })

  it('accepts per-condition parens: (attribute_not_exists(pk)) AND (attribute_not_exists(#s))', async () => {
    const pk = freshKeys[0]
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, data: { S: 'a' } },
        ConditionExpression: '(attribute_not_exists(pk)) AND (attribute_not_exists(#s))',
        ExpressionAttributeNames: { '#s': 'status' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('a')
  })

  it('accepts full-expression wrap: (attribute_not_exists(pk) AND attribute_not_exists(#s))', async () => {
    const pk = freshKeys[1]
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, data: { S: 'b' } },
        ConditionExpression: '(attribute_not_exists(pk) AND attribute_not_exists(#s))',
        ExpressionAttributeNames: { '#s': 'status' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('b')
  })

  it('accepts non-redundant nested parens: (attribute_not_exists(pk) AND (attribute_not_exists(#s)))', async () => {
    const pk = freshKeys[2]
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, data: { S: 'c' } },
        ConditionExpression: '(attribute_not_exists(pk) AND (attribute_not_exists(#s)))',
        ExpressionAttributeNames: { '#s': 'status' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('c')
  })

  it('rejects PutItem when parenthesised condition fails on seeded key', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: seeded }, data: { S: 'should-not-overwrite' } },
            ConditionExpression: '(attribute_exists(pk)) AND (#s = :wrong)',
            ExpressionAttributeNames: { '#s': 'status' },
            ExpressionAttributeValues: { ':wrong': { S: 'inactive' } },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })
})
