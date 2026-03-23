import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const PREFIX = 'lim-is-'
const keysToClean: { pk: { S: string } }[] = []

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToClean)
})

function key(id: string) {
  const k = { pk: { S: `${PREFIX}${id}` } }
  keysToClean.push(k)
  return k
}

describe('Item size limit (400KB)', () => {
  // DynamoDB item size limit is 400KB = 400 * 1024 = 409,600 bytes

  it('item just under 400KB succeeds', async () => {
    // Use a string well under 400,000 bytes total
    const k = key('under')
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { ...k, data: { S: 'x'.repeat(390_000) } },
      }),
    )

    const get = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: k,
        ConsistentRead: true,
      }),
    )
    expect(get.Item).toBeDefined()
    expect(get.Item!.data.S).toHaveLength(390_000)
  })

  it('item over 400KB fails with ValidationException', async () => {
    const k = key('over')
    // 410,000 char string = 410,000 bytes (ASCII) + key/attr overhead = over 409,600 byte limit
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, bigval: { S: 'x'.repeat(410_000) } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })

  it('size calculation includes attribute names', async () => {
    // Use many attributes with long names to push over limit
    // 100 attributes with 200-byte names = 20,000 bytes of names alone
    // 100 attributes with ~3,900-byte values = 390,000 bytes of values
    // Total ~ 410,000+ bytes > 400,000 limit
    const k = key('attrnames')
    const item: Record<string, { S: string }> = {}
    for (let i = 0; i < 100; i++) {
      const attrName = `attr${'a'.repeat(200)}${String(i).padStart(3, '0')}`
      item[attrName] = { S: 'x'.repeat(3_900) }
    }
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, ...item },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })

  it('size includes nested map attribute names and values', async () => {
    const k = key('nested')
    // A deeply nested map with large values — over 400,000 bytes total
    const nestedMap = {
      M: {
        level1: {
          M: {
            level2: {
              M: {
                payload: { S: 'x'.repeat(410_000) },
              },
            },
          },
        },
      },
    }
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, nested: nestedMap },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })

  it('size includes set elements', async () => {
    const k = key('set')
    // String set with many large elements — over 400,000 bytes total
    const elements = Array.from({ length: 100 }, (_, i) =>
      'x'.repeat(4_200) + String(i),
    )
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, bigset: { SS: elements } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })

  it('size includes list elements', async () => {
    const k = key('list')
    const elements = Array.from({ length: 100 }, (_, i) => ({
      S: 'x'.repeat(4_200) + String(i),
    }))
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, biglist: { L: elements } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })

  it('large binary attribute approaching limit succeeds', async () => {
    const k = key('binary')
    // 350,000 bytes of binary data — safely under 409,600 byte limit
    const buf = new Uint8Array(350_000)
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { ...k, bindata: { B: buf } },
      }),
    )

    const get = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: k,
        ConsistentRead: true,
      }),
    )
    expect(get.Item).toBeDefined()
    expect(get.Item!.bindata.B).toBeDefined()
  })

  it('large binary attribute over limit fails', async () => {
    const k = key('binary-over')
    // 410,000 bytes — over the 409,600 byte limit
    const buf = new Uint8Array(410_000)
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { ...k, bindata: { B: buf } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect((e as any).name).toBe('ValidationException')
      expect((e as any).message).toMatch(
        /[Ii]tem size has exceeded the maximum allowed size|[Ii]tem size to update has exceeded the maximum allowed size/,
      )
    }
  })
})
