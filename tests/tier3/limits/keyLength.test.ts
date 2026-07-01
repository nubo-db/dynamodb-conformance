import { PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError, cleanupItems } from '../../../src/helpers.js'

// Real DynamoDB caps a partition key at 2048 bytes. The ceiling is enforced on
// the read/lookup path as well as on writes.
describe('Key-value length limits', { tags: ['put-item', 'get-item', 'data-plane'] }, () => {
  const atLimit = 'a'.repeat(2048)
  const overLimit = 'a'.repeat(2049)

  it('accepts a partition key at the 2048-byte limit', async () => {
    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: { S: atLimit } } }),
    )
    await cleanupItems(hashTableDef.name, [{ pk: { S: atLimit } }])
  })

  it('rejects a partition key over the 2048-byte limit on write', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: { S: overLimit } } }),
        ),
      'ValidationException',
    )
  })

  it('rejects an over-limit partition key on the read path', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new GetItemCommand({ TableName: hashTableDef.name, Key: { pk: { S: overLimit } } }),
        ),
      'ValidationException',
    )
  })
})
