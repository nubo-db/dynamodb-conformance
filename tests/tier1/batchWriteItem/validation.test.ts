import { BatchWriteItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('BatchWriteItem — validation', () => {
  it('rejects more than 25 items', async () => {
    const items = Array.from({ length: 26 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `bw-limit-${i}` } },
      },
    }))

    await expectDynamoError(
      () => ddb.send(
        new BatchWriteItemCommand({
          RequestItems: { [hashTableDef.name]: items },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects empty RequestItems', async () => {
    await expectDynamoError(
      () => ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {},
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects writes to a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {
            _conformance_nonexistent_table: [
              {
                PutRequest: { Item: { pk: { S: 'test' } } },
              },
            ],
          },
        }),
      ),
      'ResourceNotFoundException',
    )
  })

  it('rejects duplicate keys in the same table batch', async () => {
    await expectDynamoError(
      () => ddb.send(new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'dup-key' }, val: { S: 'first' } } } },
            { PutRequest: { Item: { pk: { S: 'dup-key' }, val: { S: 'second' } } } },
          ],
        },
      })),
      'ValidationException',
    )
  })

  it('rejects a key-less item with a 400 ValidationException, not a 500', async () => {
    try {
      await ddb.send(
        new BatchWriteItemCommand({
          RequestItems: {
            [hashTableDef.name]: [
              { PutRequest: { Item: { notkey: { S: 'x' } } } },
            ],
          },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      // A too-lenient target returns 500 InternalError here; AWS rejects with
      // a 400 ValidationException for the schema-mismatched item.
      const err = e as { name?: string; $metadata?: { httpStatusCode?: number } }
      expect(err.name).toBe('ValidationException')
      expect(err.$metadata?.httpStatusCode).toBe(400)
    }
  })
})
