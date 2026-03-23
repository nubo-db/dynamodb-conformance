import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

describe('Query — LSI', () => {
  const items = [
    {
      pk: { S: 'lsi-q-1' },
      sk: { S: 'a' },
      lsi1sk: { S: 'alpha' },
      lsi2sk: { S: 'x-100' },
      data: { S: 'item1' },
    },
    {
      pk: { S: 'lsi-q-1' },
      sk: { S: 'b' },
      lsi1sk: { S: 'alpha-2' },
      lsi2sk: { S: 'x-200' },
      data: { S: 'item2' },
    },
    {
      pk: { S: 'lsi-q-1' },
      sk: { S: 'c' },
      lsi1sk: { S: 'beta' },
      lsi2sk: { S: 'x-150' },
      data: { S: 'item3' },
    },
    {
      pk: { S: 'lsi-q-2' },
      sk: { S: 'a' },
      lsi1sk: { S: 'alpha' },
      lsi2sk: { S: 'x-300' },
      data: { S: 'other-partition' },
    },
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

  it('queries LSI with equality on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk AND lsi1sk = :sk',
        ExpressionAttributeValues: {
          ':pk': { S: 'lsi-q-1' },
          ':sk': { S: 'alpha' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].data?.S).toBe('item1')
  })

  it('queries LSI with begins_with on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk AND begins_with(lsi1sk, :prefix)',
        ExpressionAttributeValues: {
          ':pk': { S: 'lsi-q-1' },
          ':prefix': { S: 'alpha' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const sortKeys = result.Items!.map((i) => i.lsi1sk.S)
    expect(sortKeys).toContain('alpha')
    expect(sortKeys).toContain('alpha-2')
  })

  it('queries LSI with BETWEEN on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi2',
        KeyConditionExpression: 'pk = :pk AND lsi2sk BETWEEN :lo AND :hi',
        ExpressionAttributeValues: {
          ':pk': { S: 'lsi-q-1' },
          ':lo': { S: 'x-100' },
          ':hi': { S: 'x-200' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items!.length).toBe(3)
    const sortKeys = result.Items!.map((i) => i.lsi2sk.S)
    expect(sortKeys).toContain('x-100')
    expect(sortKeys).toContain('x-150')
    expect(sortKeys).toContain('x-200')
  })

  it('supports ConsistentRead on LSI (unlike GSI)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'lsi-q-1' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items!.length).toBe(3)
  })

  it('ALL projection returns all base table attributes', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk AND lsi1sk = :sk',
        ExpressionAttributeValues: {
          ':pk': { S: 'lsi-q-1' },
          ':sk': { S: 'alpha' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    const item = result.Items![0]
    // ALL projection should include every attribute from the base table
    expect(item.pk?.S).toBe('lsi-q-1')
    expect(item.sk?.S).toBe('a')
    expect(item.lsi1sk?.S).toBe('alpha')
    expect(item.lsi2sk?.S).toBe('x-100')
    expect(item.data?.S).toBe('item1')
  })

  it('INCLUDE projection returns only specified non-key attributes plus keys', async () => {
    // lsi2 has projectionType INCLUDE with nonKeyAttributes ['lsi1sk']
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi2',
        KeyConditionExpression: 'pk = :pk AND lsi2sk = :sk',
        ExpressionAttributeValues: {
          ':pk': { S: 'lsi-q-1' },
          ':sk': { S: 'x-100' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    const item = result.Items![0]
    // Keys should always be present
    expect(item.pk?.S).toBe('lsi-q-1')
    expect(item.sk?.S).toBe('a')
    expect(item.lsi2sk?.S).toBe('x-100')
    // Included non-key attribute
    expect(item.lsi1sk?.S).toBe('alpha')
    // 'data' is NOT in the INCLUDE list, so it should be absent
    expect(item.data).toBeUndefined()
  })

  it('returns items only from the queried partition', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'lsi-q-1' } },
        ConsistentRead: true,
      }),
    )

    // Should only get items from partition lsi-q-1, not lsi-q-2
    const pks = result.Items!.map((i) => i.pk.S)
    expect(pks.every((pk) => pk === 'lsi-q-1')).toBe(true)
    expect(result.Items!.length).toBe(3)
  })

  it('returns empty results for non-existent partition key on LSI', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'does-not-exist' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(0)
    expect(result.Count).toBe(0)
  })
})
