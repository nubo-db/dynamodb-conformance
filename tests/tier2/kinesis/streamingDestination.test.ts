import {
  CreateTableCommand,
  EnableKinesisStreamingDestinationCommand,
  DescribeKinesisStreamingDestinationCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'
import { isUnsupportedFault, withKinesisStream } from '../../../src/infra.js'

// Kinesis streaming destination against a real provisioned stream. Enabling is
// asynchronous and ENABLING -> ACTIVE takes minutes, so assert the destination
// is registered against the stream rather than waiting for ACTIVE. The stream
// is torn down by the harness and the destination by deleting the table.

describe('Kinesis streaming destination', () => {
  const table = uniqueTableName('kds')
  let supported = true

  beforeAll(async () => {
    await ddb.send(
      new CreateTableCommand({
        TableName: table,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(table)
    try {
      await ddb.send(new DescribeKinesisStreamingDestinationCommand({ TableName: table }))
    } catch (e) {
      if (isUnsupportedFault(e)) supported = false
      else throw e
    }
  }, 120_000)

  afterAll(async () => {
    await deleteTable(table) // removes any streaming destination with it
  })

  it('enables a streaming destination and reports it via Describe', async ({ skip }) => {
    if (!supported) return skip()
    await withKinesisStream(async (streamArn) => {
      const enabled = await ddb.send(
        new EnableKinesisStreamingDestinationCommand({ TableName: table, StreamArn: streamArn }),
      )
      expect(enabled.StreamArn).toBe(streamArn)
      expect(['ENABLING', 'ACTIVE']).toContain(enabled.DestinationStatus)

      const desc = await ddb.send(
        new DescribeKinesisStreamingDestinationCommand({ TableName: table }),
      )
      const dest = (desc.KinesisDataStreamDestinations ?? []).find(
        (d) => d.StreamArn === streamArn,
      )
      expect(dest).toBeDefined()
      expect(['ENABLING', 'ACTIVE']).toContain(dest!.DestinationStatus)
    })
  }, 240_000)
})
