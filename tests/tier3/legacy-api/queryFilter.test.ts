import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('Legacy API — KeyConditions and QueryFilter', () => {
  const pk = 'legacy-query'
  const items = [
    { pk: { S: pk }, sk: { S: 'alpha' }, val: { N: '1' }, tag: { S: 'hello-world' } },
    { pk: { S: pk }, sk: { S: 'beta' }, val: { N: '2' }, tag: { S: 'hello-there' } },
    { pk: { S: pk }, sk: { S: 'gamma' }, val: { N: '3' }, tag: { S: 'goodbye' } },
    { pk: { S: pk }, sk: { S: 'delta' }, val: { N: '4' }, tag: { S: 'hello-again' } },
    { pk: { S: pk }, sk: { S: 'epsilon' }, val: { N: '5' }, tag: { S: 'world' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('KeyConditions with ComparisonOperator EQ on hash key — basic query', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(5)
    expect(result.Count).toBe(5)
  })

  it('KeyConditions with range key using BEGINS_WITH', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
          sk: {
            ComparisonOperator: 'BEGINS_WITH',
            AttributeValueList: [{ S: 'al' }],
          },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('alpha')
  })

  it('KeyConditions with range key using BETWEEN', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
          sk: {
            ComparisonOperator: 'BETWEEN',
            AttributeValueList: [{ S: 'beta' }, { S: 'epsilon' }],
          },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['beta', 'delta', 'epsilon'])
  })

  it('KeyConditions with range key using GT', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
          sk: {
            ComparisonOperator: 'GT',
            AttributeValueList: [{ S: 'delta' }],
          },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['epsilon', 'gamma'])
  })

  it('QueryFilter with ComparisonOperator EQ on non-key attribute', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
        },
        QueryFilter: {
          val: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ N: '3' }],
          },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('gamma')
    expect(result.ScannedCount).toBe(5)
  })

  it('QueryFilter with ComparisonOperator CONTAINS on string attribute', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
        },
        QueryFilter: {
          tag: {
            ComparisonOperator: 'CONTAINS',
            AttributeValueList: [{ S: 'hello' }],
          },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
    expect(result.ScannedCount).toBe(5)
  })

  it('Mixing KeyConditions with KeyConditionExpression throws ValidationException', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new QueryCommand({
            TableName: compositeTableDef.name,
            KeyConditions: {
              pk: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [{ S: pk }],
              },
            },
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: { ':pk': { S: pk } },
          }),
        ),
      'ValidationException',
    )
  })

  it('ScanIndexForward works with KeyConditions', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: pk }],
          },
        },
        ScanIndexForward: false,
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['gamma', 'epsilon', 'delta', 'beta', 'alpha'])
  })
})
