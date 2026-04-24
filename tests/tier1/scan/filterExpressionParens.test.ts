import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems, waitForGsiConsistency } from '../../../src/helpers.js'

// Scan FilterExpression parser is distinct from KeyConditionExpression and
// may also differ from Query's FilterExpression path in some emulators.
// Items carry a unique `lsi1sk` marker so scans can isolate this test's
// data from whatever else is in the shared compositeTableDef.
describe('Scan — FilterExpression parens', () => {
  const marker = 'fes-parens-marker'
  const items = [
    { pk: { S: 'fes-parens-1' }, sk: { S: 'x' }, lsi1sk: { S: marker }, type: { S: 'alpha' }, status: { S: 'active' } },
    { pk: { S: 'fes-parens-2' }, sk: { S: 'x' }, lsi1sk: { S: marker }, type: { S: 'beta' }, status: { S: 'inactive' } },
    { pk: { S: 'fes-parens-3' }, sk: { S: 'x' }, lsi1sk: { S: marker }, type: { S: 'gamma' }, status: { S: 'active' } },
    { pk: { S: 'fes-parens-4' }, sk: { S: 'x' }, lsi1sk: { S: marker }, type: { S: 'alpha' }, status: { S: 'active' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
    )
    await waitForGsiConsistency({
      tableName: compositeTableDef.name,
      indexName: 'gsi1',
      partitionKey: { name: 'lsi1sk', value: { S: marker } },
      expectedCount: items.length,
    })
  }, 30_000)

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('base table — per-condition parens: (#t = :a) OR (#t = :b)', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        FilterExpression: '(#m = :m) AND ((#t = :a) OR (#t = :b))',
        ExpressionAttributeNames: { '#m': 'lsi1sk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':m': { S: marker },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
        ConsistentRead: true,
      }),
    )

    // items 1, 2, 4 match (alpha, beta, alpha)
    expect(result.Items).toHaveLength(3)
  })

  it('base table — full-expression wrap: (#t = :a OR #t = :b)', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        FilterExpression: '(#m = :m) AND (#t = :a OR #t = :b)',
        ExpressionAttributeNames: { '#m': 'lsi1sk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':m': { S: marker },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
  })

  it('base table — non-redundant nested parens: (#t = :a OR (#t = :b))', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        FilterExpression: '(#m = :m) AND (#t = :a OR (#t = :b))',
        ExpressionAttributeNames: { '#m': 'lsi1sk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':m': { S: marker },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
  })

  it('GSI scan — parens filter returns matching items', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        FilterExpression: '(#m = :m) AND ((#t = :a) OR (#t = :b))',
        ExpressionAttributeNames: { '#m': 'lsi1sk', '#t': 'type' },
        ExpressionAttributeValues: {
          ':m': { S: marker },
          ':a': { S: 'alpha' },
          ':b': { S: 'beta' },
        },
      }),
    )

    // Same three items show through the GSI (all four have lsi1sk=marker)
    expect(result.Items).toHaveLength(3)
  })

  it('accepts NOT inside parens: (NOT (#s = :s))', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        FilterExpression: '(#m = :m) AND (NOT (#s = :s))',
        ExpressionAttributeNames: { '#m': 'lsi1sk', '#s': 'status' },
        ExpressionAttributeValues: {
          ':m': { S: marker },
          ':s': { S: 'active' },
        },
        ConsistentRead: true,
      }),
    )

    // Only item 2 has status=inactive
    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].pk.S).toBe('fes-parens-2')
  })
})
