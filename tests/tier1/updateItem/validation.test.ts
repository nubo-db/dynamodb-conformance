import { UpdateItemCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('UpdateItem — validation', () => {
  it('rejects update on non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: '_conformance_nonexistent_table',
          Key: { pk: { S: 'test' } },
          UpdateExpression: 'SET x = :v',
          ExpressionAttributeValues: { ':v': { S: 'test' } },
        }),
      ),
      'ResourceNotFoundException',
    )
  })

  it('rejects update that modifies the hash key', async () => {
    await expectDynamoError(
      () => ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'upd-val' } },
          UpdateExpression: 'SET pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'new-pk' } },
        }),
      ),
      'ValidationException',
    )
  })
})
