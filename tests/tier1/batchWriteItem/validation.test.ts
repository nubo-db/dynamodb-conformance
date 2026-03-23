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
            this_table_does_not_exist_xyz: [
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
})
