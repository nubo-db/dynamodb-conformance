import { QueryCommand, PutItemCommand } from '@aws-sdk/client-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { declareTables, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

declareTables(compositeTableDef)

const TABLE = compositeTableDef.name
const PK = 'query-cc'
const keysToClean: Record<string, AttributeValue>[] = []

// The Query half of the same rule as tests/tier1/scan/consumedCapacity.test.ts.
// A Query is bounded by its key condition, so what is measured here is that the
// filter and the projection, which are not key conditions, move nothing.
describe('ConsumedCapacity — a Query is sized before the filter and the projection', { tags: ['query', 'data-plane'] }, () => {
  beforeAll(async () => {
    for (let i = 0; i < 6; i++) {
      const key = { pk: { S: PK }, sk: { S: `s${i}` } }
      keysToClean.push(key)
      await ddb.send(new PutItemCommand({
        TableName: TABLE,
        Item: { ...key, keep: { S: i === 0 ? 'yes' : 'no' }, filler: { S: 'x'.repeat(200) } },
      }))
    }
  })

  afterAll(async () => {
    await cleanupItems(TABLE, keysToClean)
  })

  async function queryUnits(extra: Record<string, unknown> = {}) {
    const { ExpressionAttributeValues: extraValues, ...rest } = extra
    const res = await ddb.send(new QueryCommand({
      TableName: TABLE,
      KeyConditionExpression: 'pk = :p',
      ReturnConsumedCapacity: 'TOTAL',
      ...rest,
      // Merged last, and after the spread, so a caller adding a filter value
      // does not drop the key condition's own.
      ExpressionAttributeValues: {
        ':p': { S: PK },
        ...((extraValues as Record<string, AttributeValue>) ?? {}),
      },
    }))
    return { units: res.ConsumedCapacity?.CapacityUnits ?? 0, count: res.Count ?? 0 }
  }

  it('a filter matching one row costs what the unfiltered query costs', async () => {
    const plain = await queryUnits()
    const filtered = await queryUnits({
      FilterExpression: 'keep = :k',
      ExpressionAttributeValues: { ':k': { S: 'yes' } },
    })
    expect(plain.count).toBe(6)
    expect(filtered.count).toBe(1)
    expect(filtered.units).toBe(plain.units)
  })

  it('a filter matching nothing costs the same again', async () => {
    const plain = await queryUnits()
    const empty = await queryUnits({
      FilterExpression: 'keep = :k',
      ExpressionAttributeValues: { ':k': { S: 'no-such-value' } },
    })
    expect(empty.count).toBe(0)
    expect(empty.units).toBe(plain.units)
  })

  it('projecting the sort key alone costs what selecting everything costs', async () => {
    const plain = await queryUnits()
    const projected = await queryUnits({ ProjectionExpression: 'sk' })
    expect(projected.units).toBe(plain.units)
  })
})
