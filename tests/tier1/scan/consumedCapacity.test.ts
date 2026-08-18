import { ScanCommand, PutItemCommand } from '@aws-sdk/client-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { declareTables, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

declareTables(compositeTableDef)

const TABLE = compositeTableDef.name
const PK = 'scan-cc'
const keysToClean: Record<string, AttributeValue>[] = []

// DynamoDB sizes a read before the WHERE clause and before the projection, so a
// filter that discards most rows saves nothing and a narrow projection saves
// nothing either. An engine summing the rows it is about to hand back reports a
// smaller figure and looks cheaper than it is.
//
// Every assertion compares two figures measured against the same table moments
// apart, rather than pinning a byte count, so it survives an unrelated change to
// the fixture. The suite runs single-fork and sequential, so nothing writes
// between the two reads of a pair.
describe('ConsumedCapacity — a Scan is sized before the filter and the projection', { tags: ['scan', 'data-plane'] }, () => {
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

  async function scanUnits(extra: Record<string, unknown> = {}) {
    const res = await ddb.send(new ScanCommand({
      TableName: TABLE, ReturnConsumedCapacity: 'TOTAL', ...extra,
    }))
    return { units: res.ConsumedCapacity?.CapacityUnits ?? 0, count: res.Count ?? 0 }
  }

  it('a filter matching one row costs what the unfiltered scan costs', async () => {
    const plain = await scanUnits()
    const filtered = await scanUnits({
      FilterExpression: 'keep = :k',
      ExpressionAttributeValues: { ':k': { S: 'yes' } },
    })
    expect(filtered.count).toBeLessThan(plain.count)
    expect(filtered.units).toBe(plain.units)
  })

  it('a filter matching nothing costs the same again', async () => {
    const plain = await scanUnits()
    const empty = await scanUnits({
      FilterExpression: 'keep = :k',
      ExpressionAttributeValues: { ':k': { S: 'no-such-value' } },
    })
    expect(empty.count).toBe(0)
    expect(empty.units).toBe(plain.units)
  })

  it('a narrow projection costs what selecting everything costs', async () => {
    const plain = await scanUnits()
    const projected = await scanUnits({ ProjectionExpression: 'pk' })
    expect(projected.units).toBe(plain.units)
  })

  it('a filter and a projection together still cost the same', async () => {
    const plain = await scanUnits()
    const both = await scanUnits({
      ProjectionExpression: 'pk',
      FilterExpression: 'keep = :k',
      ExpressionAttributeValues: { ':k': { S: 'yes' } },
    })
    expect(both.units).toBe(plain.units)
  })
})
