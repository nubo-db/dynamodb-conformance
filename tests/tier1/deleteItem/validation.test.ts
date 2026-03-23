import { DeleteItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { expectDynamoError } from '../../../src/helpers.js'

describe('DeleteItem — validation', () => {
  it('rejects DeleteItem on a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new DeleteItemCommand({
          TableName: 'this_table_does_not_exist_xyz',
          Key: { pk: { S: 'test' } },
        }),
      ),
      'ResourceNotFoundException',
    )
  })
})
