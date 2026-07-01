import {
  PutItemCommand,
  QueryCommand,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  createTable,
  deleteTable,
  uniqueTableName,
  waitForGsiConsistency,
} from '../../../src/helpers.js'
import type { TestTableDef } from '../../../src/types.js'

// A hash-only GSI (no range key) on a composite-key base table. Many items share
// the GSI hash and the base partition key, differing only by the base sort key,
// so a continuation must carry the base sort key to disambiguate.
const tableDef: TestTableDef = {
  name: uniqueTableName('gho'),
  hashKey: { name: 'pk', type: 'S' },
  rangeKey: { name: 'sk', type: 'S' },
  gsis: [{ indexName: 'gho', hashKey: { name: 'gh', type: 'S' }, projectionType: 'ALL' }],
}

describe('Query — hash-only GSI pagination on a composite base table', { tags: ['query', 'data-plane', 'gsi'] }, () => {
  const gh = 'gho-shared'
  const N = 5

  beforeAll(async () => {
    await createTable(tableDef)
    for (let i = 0; i < N; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableDef.name,
          Item: { pk: { S: 'p' }, sk: { S: `s${i}` }, gh: { S: gh } },
        }),
      )
    }
    await waitForGsiConsistency({
      tableName: tableDef.name,
      indexName: 'gho',
      partitionKey: { name: 'gh', value: { S: gh } },
      expectedCount: N,
    })
  }, 120_000)

  afterAll(async () => {
    await deleteTable(tableDef.name)
  })

  it('walks every item across pages when they share the GSI hash and base partition key', async () => {
    // Real AWS returns every item exactly once across pages; the LastEvaluatedKey
    // carries the base sort key as the tie-breaker.
    const seen = new Set<string>()
    let lastKey: Record<string, AttributeValue> | undefined
    let pages = 0
    do {
      const res = await ddb.send(
        new QueryCommand({
          TableName: tableDef.name,
          IndexName: 'gho',
          KeyConditionExpression: '#gh = :gh',
          ExpressionAttributeNames: { '#gh': 'gh' },
          ExpressionAttributeValues: { ':gh': { S: gh } },
          Limit: 2,
          ExclusiveStartKey: lastKey,
        }),
      )
      for (const it of res.Items ?? []) seen.add(it.sk.S!)
      lastKey = res.LastEvaluatedKey
      pages++
    } while (lastKey && pages < 10)

    expect(seen.size).toBe(N)
  })
})
