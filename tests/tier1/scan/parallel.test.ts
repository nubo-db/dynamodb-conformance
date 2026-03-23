import {
  PutItemCommand,
  ScanCommand,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Scan — parallel', () => {
  const TOTAL_SEGMENTS = 4
  const items = Array.from({ length: 20 }, (_, i) => ({
    pk: { S: `parallel-scan-${String(i).padStart(3, '0')}` },
    val: { N: String(i) },
  }))

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

  /** Run a full parallel scan, paginating each segment to completion */
  async function parallelScan(): Promise<Record<string, AttributeValue>[][]> {
    const segmentResults: Record<string, AttributeValue>[][] = []

    await Promise.all(
      Array.from({ length: TOTAL_SEGMENTS }, async (_, segment) => {
        const segmentItems: Record<string, AttributeValue>[] = []
        let lastKey: Record<string, AttributeValue> | undefined

        do {
          const result = await ddb.send(
            new ScanCommand({
              TableName: hashTableDef.name,
              TotalSegments: TOTAL_SEGMENTS,
              Segment: segment,
              ExclusiveStartKey: lastKey,
              FilterExpression: 'begins_with(pk, :prefix)',
              ExpressionAttributeValues: {
                ':prefix': { S: 'parallel-scan-' },
              },
              ConsistentRead: true,
            }),
          )
          segmentItems.push(...(result.Items ?? []))
          lastKey = result.LastEvaluatedKey
        } while (lastKey)

        segmentResults[segment] = segmentItems
      }),
    )

    return segmentResults
  }

  it('parallel scan with segments returns disjoint results', async () => {
    const segmentResults = await parallelScan()

    // Collect all PKs per segment and verify no overlap
    const allPks = new Set<string>()
    for (const segItems of segmentResults) {
      for (const item of segItems) {
        const pk = item.pk.S!
        expect(allPks.has(pk)).toBe(false)
        allPks.add(pk)
      }
    }
  })

  it('union of all segments equals full table scan', async () => {
    const segmentResults = await parallelScan()

    // Gather all PKs from parallel scan
    const parallelPks = new Set<string>()
    for (const segItems of segmentResults) {
      for (const item of segItems) {
        parallelPks.add(item.pk.S!)
      }
    }

    // Do a full scan for comparison
    const fullItems: Record<string, AttributeValue>[] = []
    let lastKey: Record<string, AttributeValue> | undefined
    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          ExclusiveStartKey: lastKey,
          FilterExpression: 'begins_with(pk, :prefix)',
          ExpressionAttributeValues: {
            ':prefix': { S: 'parallel-scan-' },
          },
          ConsistentRead: true,
        }),
      )
      fullItems.push(...(result.Items ?? []))
      lastKey = result.LastEvaluatedKey
    } while (lastKey)

    const fullPks = new Set(fullItems.map((i) => i.pk.S!))
    expect(parallelPks.size).toBe(fullPks.size)
    for (const pk of fullPks) {
      expect(parallelPks.has(pk)).toBe(true)
    }
  })

  it('each segment returns valid items with no corruption', async () => {
    const segmentResults = await parallelScan()

    for (let seg = 0; seg < TOTAL_SEGMENTS; seg++) {
      for (const item of segmentResults[seg]) {
        // Each item should have the expected shape
        expect(item.pk).toBeDefined()
        expect(item.pk.S).toMatch(/^parallel-scan-\d{3}$/)
        expect(item.val).toBeDefined()
        expect(item.val.N).toBeDefined()
        // val should be a valid number string
        const num = Number(item.val.N)
        expect(num).toBeGreaterThanOrEqual(0)
        expect(num).toBeLessThan(20)
      }
    }
  })
})
