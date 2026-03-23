import { BatchGetItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('BatchGetItem — validation', () => {
  it('rejects more than 100 keys', async () => {
    const keys = Array.from({ length: 101 }, (_, i) => ({
      pk: { S: `bg-limit-${i}` },
    }))

    await expectDynamoError(
      () => ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: { Keys: keys, ConsistentRead: true },
          },
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects empty RequestItems', async () => {
    await expectDynamoError(
      () => ddb.send(
        new BatchGetItemCommand({
          RequestItems: {},
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects reads from a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            this_table_does_not_exist_xyz: {
              Keys: [{ pk: { S: 'test' } }],
            },
          },
        }),
      ),
      'ResourceNotFoundException',
    )
  })

  it('rejects duplicate keys in the same batch', async () => {
    await expectDynamoError(
      () => ddb.send(
        new BatchGetItemCommand({
          RequestItems: {
            [hashTableDef.name]: {
              Keys: [{ pk: { S: 'same' } }, { pk: { S: 'same' } }],
              ConsistentRead: true,
            },
          },
        }),
      ),
      'ValidationException',
      'Provided list of item keys contains duplicates',
    )
  })
})
