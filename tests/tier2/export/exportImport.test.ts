import {
  CreateTableCommand,
  DescribeTableCommand,
  PutItemCommand,
  ExportTableToPointInTimeCommand,
  DescribeExportCommand,
  ListExportsCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  enablePitr,
} from '../../../src/helpers.js'
import { isUnsupportedFault, withS3Bucket } from '../../../src/infra.js'

// Table export to S3. Export requires PITR and runs asynchronously (minutes to
// COMPLETED), so assert it initiates and is reported, not that it finishes. The
// bucket is torn down by the harness; the export job failing once the bucket is
// gone is harmless and leaves no billable resource.
//
// ImportTable coverage is held until the test account grants CloudWatch Logs
// access (logs:CreateLogGroup/CreateLogStream/PutLogEvents on
// /aws-dynamodb/imports), which the import service requires; see the gap map.

describe('Export to S3', () => {
  const source = uniqueTableName('exp_src')
  let arn = ''
  let supported = true

  beforeAll(async () => {
    await ddb.send(
      new CreateTableCommand({
        TableName: source,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(source)
    await ddb.send(new PutItemCommand({ TableName: source, Item: { pk: { S: 'a' } } }))
    arn = (await ddb.send(new DescribeTableCommand({ TableName: source }))).Table!.TableArn!
    try {
      await ddb.send(new ListExportsCommand({ TableArn: arn }))
      await enablePitr(source)
    } catch (e) {
      if (isUnsupportedFault(e)) supported = false
      else throw e
    }
  }, 180_000)

  afterAll(async () => {
    await deleteTable(source)
  })

  it('ExportTableToPointInTime initiates an export and reports it', async ({ skip }) => {
    if (!supported) return skip()
    await withS3Bucket(async (bucket) => {
      const exp = await ddb.send(
        new ExportTableToPointInTimeCommand({ TableArn: arn, S3Bucket: bucket }),
      )
      const exportArn = exp.ExportDescription?.ExportArn
      expect(exportArn).toBeTruthy()
      expect(exp.ExportDescription?.ExportStatus).toBe('IN_PROGRESS')

      const desc = await ddb.send(new DescribeExportCommand({ ExportArn: exportArn }))
      expect(desc.ExportDescription?.ExportArn).toBe(exportArn)

      const list = await ddb.send(new ListExportsCommand({ TableArn: arn }))
      expect((list.ExportSummaries ?? []).map((s) => s.ExportArn)).toContain(exportArn)
    })
  }, 240_000)
})
