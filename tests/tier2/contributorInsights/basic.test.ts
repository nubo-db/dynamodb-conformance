import {
  CreateTableCommand,
  UpdateContributorInsightsCommand,
  DescribeContributorInsightsCommand,
  ListContributorInsightsCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'
import { isUnsupportedFault } from '../../../src/infra.js'

// CloudWatch Contributor Insights for a table. Enable/describe/list; disabling
// happens implicitly when the table is deleted in teardown.

describe('Contributor insights — enable/describe/list', () => {
  const table = uniqueTableName('ci')
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
      await ddb.send(new DescribeContributorInsightsCommand({ TableName: table }))
    } catch (e) {
      if (isUnsupportedFault(e)) supported = false
      else throw e
    }
  }, 120_000)

  afterAll(async () => {
    await deleteTable(table) // deleting the table disables contributor insights
  })

  it('reports DISABLED by default', async ({ skip }) => {
    if (!supported) return skip()
    const res = await ddb.send(new DescribeContributorInsightsCommand({ TableName: table }))
    expect(res.ContributorInsightsStatus).toBe('DISABLED')
  })

  it('enabling transitions the status and lists the table', async ({ skip }) => {
    if (!supported) return skip()
    await ddb.send(
      new UpdateContributorInsightsCommand({
        TableName: table,
        ContributorInsightsAction: 'ENABLE',
      }),
    )
    const desc = await ddb.send(new DescribeContributorInsightsCommand({ TableName: table }))
    expect(['ENABLING', 'ENABLED']).toContain(desc.ContributorInsightsStatus)

    const list = await ddb.send(new ListContributorInsightsCommand({}))
    const names = (list.ContributorInsightsSummaries ?? []).map((s) => s.TableName)
    expect(names).toContain(table)
  })
})
