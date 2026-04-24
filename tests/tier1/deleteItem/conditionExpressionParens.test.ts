import {
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError, cleanupItems } from '../../../src/helpers.js'

// Exercises the DeleteItem ConditionExpression parser with parenthesised forms.
describe('DeleteItem — ConditionExpression parens', () => {
  const seedKeys = [
    'del-cep-percond',
    'del-cep-fullwrap',
    'del-cep-nested',
    'del-cep-fail',
  ]

  beforeAll(async () => {
    await Promise.all(
      seedKeys.map((k) =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: k }, status: { S: 'active' } },
          }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      seedKeys.map((k) => ({ pk: { S: k } })),
    )
  })

  it('accepts per-condition parens: (attribute_exists(pk)) AND (#s = :v)', async () => {
    const pk = 'del-cep-percond'
    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConditionExpression: '(attribute_exists(pk)) AND (#s = :v)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':v': { S: 'active' } },
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('accepts full-expression wrap: (attribute_exists(pk) AND #s = :v)', async () => {
    const pk = 'del-cep-fullwrap'
    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConditionExpression: '(attribute_exists(pk) AND #s = :v)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':v': { S: 'active' } },
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('accepts non-redundant nested parens: (attribute_exists(pk) AND (#s = :v))', async () => {
    const pk = 'del-cep-nested'
    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConditionExpression: '(attribute_exists(pk) AND (#s = :v))',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':v': { S: 'active' } },
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('rejects DeleteItem when parenthesised condition fails; item remains', async () => {
    const pk = 'del-cep-fail'
    await expectDynamoError(
      () =>
        ddb.send(
          new DeleteItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: pk } },
            ConditionExpression: '(attribute_exists(pk)) AND (#s = :wrong)',
            ExpressionAttributeNames: { '#s': 'status' },
            ExpressionAttributeValues: { ':wrong': { S: 'inactive' } },
          }),
        ),
      'ConditionalCheckFailedException',
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeDefined()
    expect(check.Item!.status.S).toBe('active')
  })
})
