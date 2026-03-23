import {
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('Legacy API — Expected (legacy ConditionExpression)', () => {
  const keys = [
    { pk: { S: 'expected-1' } },
    { pk: { S: 'expected-2' } },
    { pk: { S: 'expected-3' } },
    { pk: { S: 'expected-4' } },
    { pk: { S: 'expected-5' } },
    { pk: { S: 'expected-6' } },
    { pk: { S: 'expected-7' } },
    { pk: { S: 'expected-8' } },
    { pk: { S: 'expected-9' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, keys)
  })

  it('PutItem with Expected Exists:false succeeds when item does not exist', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-1' }, data: { S: 'created' } },
        Expected: {
          pk: { Exists: false },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-1' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item).toBeDefined()
    expect(got.Item!.data.S).toBe('created')
  })

  it('PutItem with Expected Exists:false fails when item already exists', async () => {
    // Ensure item exists
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-2' }, data: { S: 'existing' } },
      }),
    )

    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'expected-2' }, data: { S: 'overwrite' } },
            Expected: {
              pk: { Exists: false },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('PutItem with Expected ComparisonOperator EQ succeeds on match', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-3' }, status: { S: 'active' } },
      }),
    )

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-3' }, status: { S: 'updated' } },
        Expected: {
          status: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'active' }],
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-3' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.status.S).toBe('updated')
  })

  it('PutItem with Expected ComparisonOperator EQ fails on mismatch', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-4' }, status: { S: 'active' } },
      }),
    )

    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'expected-4' }, status: { S: 'changed' } },
            Expected: {
              status: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [{ S: 'wrong-value' }],
              },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('DeleteItem with Expected Exists:true succeeds when attr exists', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-5' }, data: { S: 'to-delete' } },
      }),
    )

    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-5' } },
        Expected: {
          data: { Exists: true, Value: { S: 'to-delete' } },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-5' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item).toBeUndefined()
  })

  it('DeleteItem with Expected Exists:false fails when attr exists', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-6' }, data: { S: 'present' } },
      }),
    )

    await expectDynamoError(
      () =>
        ddb.send(
          new DeleteItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'expected-6' } },
            Expected: {
              data: { Exists: false },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('PutItem with ConditionalOperator AND — both conditions must pass', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-7' }, status: { S: 'active' }, role: { S: 'admin' } },
      }),
    )

    // Both match — should succeed
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-7' }, status: { S: 'done' }, role: { S: 'admin' } },
        Expected: {
          status: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'active' }],
          },
          role: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'admin' }],
          },
        },
        ConditionalOperator: 'AND',
      }),
    )

    // One mismatches — should fail
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'expected-7' }, status: { S: 'fail' }, role: { S: 'admin' } },
            Expected: {
              status: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [{ S: 'active' }],
              },
              role: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [{ S: 'admin' }],
              },
            },
            ConditionalOperator: 'AND',
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('PutItem with ConditionalOperator OR — either condition can pass', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-8' }, status: { S: 'active' }, role: { S: 'user' } },
      }),
    )

    // First matches, second doesn't — should succeed with OR
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-8' }, status: { S: 'updated' }, role: { S: 'user' } },
        Expected: {
          status: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'active' }],
          },
          role: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'admin' }],
          },
        },
        ConditionalOperator: 'OR',
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-8' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.status.S).toBe('updated')
  })

  it('Mixing Expected with ConditionExpression throws ValidationException', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'expected-9' }, data: { S: 'test' } },
            Expected: {
              pk: { Exists: false },
            },
            ConditionExpression: 'attribute_not_exists(pk)',
          }),
        ),
      'ValidationException',
    )
  })

  it('Expected with ComparisonOperator BEGINS_WITH succeeds on match', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-9' }, name: { S: 'hello-world' } },
      }),
    )

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'expected-9' }, name: { S: 'hello-updated' } },
        Expected: {
          name: {
            ComparisonOperator: 'BEGINS_WITH',
            AttributeValueList: [{ S: 'hello' }],
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'expected-9' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.name.S).toBe('hello-updated')
  })
})
