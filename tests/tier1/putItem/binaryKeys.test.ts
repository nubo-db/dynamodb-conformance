import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeBTableDef, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — binary sort key', () => {
  const bin1 = new Uint8Array([0x01, 0x02, 0x03])
  const bin2 = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF])
  const bin3 = new Uint8Array([0xFF, 0x00, 0x7F, 0x80])

  afterAll(async () => {
    await cleanupItems(compositeBTableDef.name, [
      { pk: { S: 'binput-1' }, sk: { B: bin1 } },
      { pk: { S: 'binput-2' }, sk: { B: bin2 } },
      { pk: { S: 'binput-2' }, sk: { B: bin3 } },
    ])
  })

  it('puts and retrieves an item with binary sort key', async () => {
    const key = { pk: { S: 'binput-1' }, sk: { B: bin1 } }

    await ddb.send(
      new PutItemCommand({
        TableName: compositeBTableDef.name,
        Item: { ...key, data: { S: 'binary-item' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeBTableDef.name,
        Key: key,
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.data.S).toBe('binary-item')
  })

  it('puts items with different binary sort key values', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: compositeBTableDef.name,
        Item: { pk: { S: 'binput-2' }, sk: { B: bin2 }, label: { S: 'deadbeef' } },
      }),
    )

    await ddb.send(
      new PutItemCommand({
        TableName: compositeBTableDef.name,
        Item: { pk: { S: 'binput-2' }, sk: { B: bin3 }, label: { S: 'ff007f80' } },
      }),
    )

    const r1 = await ddb.send(
      new GetItemCommand({
        TableName: compositeBTableDef.name,
        Key: { pk: { S: 'binput-2' }, sk: { B: bin2 } },
        ConsistentRead: true,
      }),
    )
    expect(r1.Item).toBeDefined()
    expect(r1.Item!.label.S).toBe('deadbeef')

    const r2 = await ddb.send(
      new GetItemCommand({
        TableName: compositeBTableDef.name,
        Key: { pk: { S: 'binput-2' }, sk: { B: bin3 } },
        ConsistentRead: true,
      }),
    )
    expect(r2.Item).toBeDefined()
    expect(r2.Item!.label.S).toBe('ff007f80')
  })

  it('binary key round-trip preserves exact bytes', async () => {
    const bytes = new Uint8Array([0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF])
    const key = { pk: { S: 'binput-1' }, sk: { B: bin1 } }

    // Reuse bin1 item, update with binary attribute
    await ddb.send(
      new PutItemCommand({
        TableName: compositeBTableDef.name,
        Item: { ...key, payload: { B: bytes } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeBTableDef.name,
        Key: key,
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    const returned = new Uint8Array(result.Item!.sk.B as unknown as ArrayBuffer)
    expect(returned).toEqual(bin1)

    const returnedPayload = new Uint8Array(result.Item!.payload.B as unknown as ArrayBuffer)
    expect(returnedPayload).toEqual(bytes)
  })
})
