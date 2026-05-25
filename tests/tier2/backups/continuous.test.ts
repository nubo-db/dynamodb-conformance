import {
  CreateTableCommand,
  DescribeContinuousBackupsCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  enablePitr,
} from '../../../src/helpers.js'
import { isUnsupportedFault } from '../../../src/infra.js'

// Continuous backups / point-in-time recovery. PITR is DISABLED by default and
// transitions to ENABLED once turned on. (ContinuousBackupsStatus is enabled
// asynchronously after table creation, so it is transient right after ACTIVE
// and not asserted here.) Restore-to-point-in-time is deferred: the earliest
// restorable time lags table creation by minutes, so it cannot be exercised
// within a test run (recorded in the gap map).

describe('Continuous backups — PITR', () => {
  const table = uniqueTableName('cb')
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
      await ddb.send(new DescribeContinuousBackupsCommand({ TableName: table }))
    } catch (e) {
      if (isUnsupportedFault(e)) supported = false
      else throw e
    }
  }, 120_000)

  afterAll(async () => {
    await deleteTable(table) // deleting the table removes PITR with it
  })

  it('reports PITR DISABLED by default', async ({ skip }) => {
    if (!supported) return skip()
    const res = await ddb.send(new DescribeContinuousBackupsCommand({ TableName: table }))
    const d = res.ContinuousBackupsDescription!
    expect(d.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus).toBe('DISABLED')
  })

  it('enabling PITR transitions PointInTimeRecoveryStatus to ENABLED', async ({ skip }) => {
    if (!supported) return skip()
    await enablePitr(table)
    const res = await ddb.send(new DescribeContinuousBackupsCommand({ TableName: table }))
    expect(
      res.ContinuousBackupsDescription?.PointInTimeRecoveryDescription
        ?.PointInTimeRecoveryStatus,
    ).toBe('ENABLED')
  })
})
