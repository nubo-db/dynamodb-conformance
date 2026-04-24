import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — KeyConditionExpression with parentheses', () => {
  const pk = 'kce-parens'
  const items = [
    { pk: { S: pk }, sk: { S: '1' }, data: { S: 'a' } },
    { pk: { S: pk }, sk: { S: '2' }, data: { S: 'b' } },
    { pk: { S: pk }, sk: { S: '3' }, data: { S: 'c' } },
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

  it('accepts parenthesized sub-expressions in KeyConditionExpression', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '(#pk = :pk) AND (#sk = :sk)',
        ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sk': { S: '1' },
        },
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].data?.S).toBe('a')
  })

  it('accepts parentheses around the full KeyConditionExpression', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '(#pk = :pk AND #sk = :sk)',
        ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sk': { S: '2' },
        },
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].data?.S).toBe('b')
  })

  it('handles nested parentheses in KeyConditionExpression', async () => {
    // DynamoDB rejects redundant wrapping like `((x)) AND ((y))` with
    // "The expression has redundant parentheses", so we use a genuinely
    // nested form (outer parens wrap the AND, inner parens wrap one side).
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '(#pk = :pk AND (#sk = :sk))',
        ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sk': { S: '3' },
        },
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].data?.S).toBe('c')
  })
})
