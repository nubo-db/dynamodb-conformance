import { ScanCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { expectDynamoError } from '../../../src/helpers.js'

describe('Scan — validation', () => {
  it('rejects scan on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: 'this_table_does_not_exist_xyz',
          }),
        ),
      'ResourceNotFoundException',
    )
  })
})
