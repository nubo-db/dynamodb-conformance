import {
  PutItemCommand,
  GetItemCommand,
  ScanCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('GetItem — projection validation and fidelity', { tags: ['get-item', 'data-plane'] }, () => {
  const pk = 'proj-val'
  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])
  })

  const seed = () =>
    ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          a: { M: { b: { S: 'bb' } } },
          l: { L: [{ S: 'l0' }, { S: 'l1' }, { S: 'l2' }] },
        },
      }),
    )

  it('rejects overlapping projection paths (a and a.b)', async () => {
    await seed()
    try {
      await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: pk } },
          ProjectionExpression: '#a, #a.#b',
          ExpressionAttributeNames: { '#a': 'a', '#b': 'b' },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })

  it('rejects duplicate projection paths (a and a)', async () => {
    try {
      await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: pk } },
          ProjectionExpression: '#a, #a',
          ExpressionAttributeNames: { '#a': 'a' },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })

  it('projects multiple list indices compacted and index-ordered', async () => {
    // Real AWS returns both requested elements as a compacted list, in index
    // order, regardless of the order they were requested.
    await seed()
    const res = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ProjectionExpression: '#l[2], #l[0]',
        ExpressionAttributeNames: { '#l': 'l' },
      }),
    )
    expect(res.Item!.l.L).toEqual([{ S: 'l0' }, { S: 'l2' }])
  })

  it('rejects an undefined projection name even when the scan matches nothing', async () => {
    // The undefined-name check fires before row evaluation, so a scan that
    // matches no rows still rejects rather than returning an empty result.
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          FilterExpression: 'pk = :never',
          ExpressionAttributeValues: { ':never': { S: 'no-such-pk-xyz-000' } },
          ProjectionExpression: '#undef',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })
})
