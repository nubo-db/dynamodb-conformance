import { QueryCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('Query — validation', () => {
  it('rejects query on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new QueryCommand({
            TableName: 'this_table_does_not_exist_xyz',
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: { ':pk': { S: 'test' } },
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('rejects query without KeyConditionExpression', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new QueryCommand({
            TableName: compositeTableDef.name,
          }),
        ),
      'ValidationException',
    )
  })
})
