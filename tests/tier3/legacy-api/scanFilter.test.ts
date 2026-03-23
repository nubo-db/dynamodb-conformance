import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('Legacy API — ScanFilter (legacy FilterExpression)', () => {
  const prefix = 'scanfilt'
  const items = [
    { pk: { S: `${prefix}-1` }, val: { N: '10' }, name: { S: 'alpha-one' } },
    { pk: { S: `${prefix}-2` }, val: { N: '20' }, name: { S: 'beta-two' } },
    { pk: { S: `${prefix}-3` }, val: { N: '30' }, name: { S: 'alpha-three' } },
    { pk: { S: `${prefix}-4` }, val: { N: '40' }, name: { S: 'gamma-four' } },
    { pk: { S: `${prefix}-5` }, val: { N: '50' }, name: { S: 'alpha-five' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  // Helper to collect all pages from a scan with ScanFilter
  async function scanWithFilter(scanFilter: Record<string, any>, conditionalOperator?: string) {
    const allItems: Record<string, any>[] = []
    let lastKey: Record<string, any> | undefined

    do {
      const params: any = {
        TableName: hashTableDef.name,
        ScanFilter: scanFilter,
        ...(conditionalOperator ? { ConditionalOperator: conditionalOperator } : {}),
        ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
      }
      const result = await ddb.send(new ScanCommand(params))
      // Only collect items that match our test prefix
      const matching = (result.Items ?? []).filter((i) => i.pk.S?.startsWith(prefix))
      allItems.push(...matching)
      lastKey = result.LastEvaluatedKey
    } while (lastKey)

    return allItems
  }

  it('ScanFilter with ComparisonOperator EQ filters results', async () => {
    const result = await scanWithFilter({
      val: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ N: '20' }],
      },
    })

    expect(result).toHaveLength(1)
    expect(result[0].pk.S).toBe(`${prefix}-2`)
  })

  it('ScanFilter with ComparisonOperator GT on numeric attribute', async () => {
    const result = await scanWithFilter({
      val: {
        ComparisonOperator: 'GT',
        AttributeValueList: [{ N: '30' }],
      },
    })

    expect(result).toHaveLength(2)
    const pks = result.map((i) => i.pk.S).sort()
    expect(pks).toEqual([`${prefix}-4`, `${prefix}-5`])
  })

  it('ScanFilter with ComparisonOperator BEGINS_WITH on string', async () => {
    const result = await scanWithFilter({
      name: {
        ComparisonOperator: 'BEGINS_WITH',
        AttributeValueList: [{ S: 'alpha' }],
      },
    })

    expect(result).toHaveLength(3)
    const pks = result.map((i) => i.pk.S).sort()
    expect(pks).toEqual([`${prefix}-1`, `${prefix}-3`, `${prefix}-5`])
  })

  it('ScanFilter with ComparisonOperator CONTAINS on string', async () => {
    const result = await scanWithFilter({
      name: {
        ComparisonOperator: 'CONTAINS',
        AttributeValueList: [{ S: 'two' }],
      },
    })

    expect(result).toHaveLength(1)
    expect(result[0].pk.S).toBe(`${prefix}-2`)
  })

  it('Multiple ScanFilter conditions with ConditionalOperator AND', async () => {
    const result = await scanWithFilter(
      {
        name: {
          ComparisonOperator: 'BEGINS_WITH',
          AttributeValueList: [{ S: 'alpha' }],
        },
        val: {
          ComparisonOperator: 'GT',
          AttributeValueList: [{ N: '20' }],
        },
      },
      'AND',
    )

    expect(result).toHaveLength(2)
    const pks = result.map((i) => i.pk.S).sort()
    expect(pks).toEqual([`${prefix}-3`, `${prefix}-5`])
  })

  it('Mixing ScanFilter with FilterExpression throws ValidationException', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            ScanFilter: {
              val: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [{ N: '10' }],
              },
            },
            FilterExpression: 'val = :v',
            ExpressionAttributeValues: { ':v': { N: '10' } },
          }),
        ),
      'ValidationException',
    )
  })
})
