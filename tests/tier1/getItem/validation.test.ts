import { GetItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('GetItem — validation', () => {
  it('rejects GetItem on a non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new GetItemCommand({
            TableName: 'this_table_does_not_exist_xyz',
            Key: { pk: { S: 'test' } },
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('rejects GetItem with missing range key on composite table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new GetItemCommand({
            TableName: compositeTableDef.name,
            Key: { pk: { S: 'test' } }, // missing sk
          }),
        ),
      'ValidationException',
    )
  })
})
