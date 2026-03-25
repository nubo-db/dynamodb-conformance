import { DeleteItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { expectDynamoError } from '../../../src/helpers.js'

describe('DeleteItem — validation', () => {
  it('rejects DeleteItem on a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new DeleteItemCommand({
          TableName: '_conformance_nonexistent_table',
          Key: { pk: { S: 'test' } },
        }),
      ),
      'ResourceNotFoundException',
    )
  })
})
