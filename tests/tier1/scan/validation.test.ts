import { ScanCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError } from '../../../src/helpers.js'

describe('Scan — validation', { tags: ['scan', 'data-plane', 'negative-path'] }, () => {
  it('rejects scan on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: '_conformance_nonexistent_table',
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('rejects TotalSegments above the maximum', async () => {
    // Real AWS caps TotalSegments at 1000000; one above is a ValidationException.
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            Segment: 0,
            TotalSegments: 1000001,
          }),
        ),
      'ValidationException',
    )
  })
})
