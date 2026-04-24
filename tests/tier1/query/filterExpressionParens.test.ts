import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

// Query FilterExpression parser is distinct from KeyConditionExpression in
// most emulators, so parens working there does not imply they work here.
describe('Query — FilterExpression parens', () => {
  const pk = 'feq-parens'
  const items = [
    { pk: { S: pk }, sk: { S: '1' }, type: { S: 'alpha' }, status: { S: 'active' }, name: { S: 'alice' } },
    { pk: { S: pk }, sk: { S: '2' }, type: { S: 'beta' }, status: { S: 'inactive' }, name: { S: 'bob' } },
    { pk: { S: pk }, sk: { S: '3' }, type: { S: 'gamma' }, status: { S: 'active' }, name: { S: 'alex' } },
    { pk: { S: pk }, sk: { S: '4' }, type: { S: 'alpha' }, status: { S: 'active' }, name: { S: 'carol' } },
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

  it('accepts per-condition parens: (#t = :a) OR (#t = :b)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '(#t = :a) OR (#t = :b)',
        ExpressionAttributeNames: { '#pk': 'pk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
      }),
    )

    // items 1, 2, and 4 match (alpha, beta, alpha)
    expect(result.Items).toHaveLength(3)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['1', '2', '4'])
  })

  it('accepts full-expression wrap: (#t = :a OR #t = :b)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '(#t = :a OR #t = :b)',
        ExpressionAttributeNames: { '#pk': 'pk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
      }),
    )

    expect(result.Items).toHaveLength(3)
  })

  it('accepts non-redundant nested parens: (#t = :a OR (#t = :b))', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '(#t = :a OR (#t = :b))',
        ExpressionAttributeNames: { '#pk': 'pk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
      }),
    )

    expect(result.Items).toHaveLength(3)
  })

  it('combines parens with begins_with: (#s = :s) AND (begins_with(#n, :p))', async () => {
    // Matches items with status=active AND name starting with 'al'
    // sk=1 alice (active, al) ✓
    // sk=3 alex (active, al) ✓
    // sk=4 carol (active, ca) ✗
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '(#s = :s) AND (begins_with(#n, :p))',
        ExpressionAttributeNames: { '#pk': 'pk', '#s': 'status', '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':s': { S: 'active' },
          ':p': { S: 'al' },
        },
      }),
    )

    expect(result.Items).toHaveLength(2)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['1', '3'])
  })
})
