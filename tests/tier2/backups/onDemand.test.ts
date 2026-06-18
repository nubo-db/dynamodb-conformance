import {
  CreateTableCommand,
  PutItemCommand,
  CreateBackupCommand,
  DescribeBackupCommand,
  ListBackupsCommand,
  DeleteBackupCommand,
  RestoreTableFromBackupCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  retryWhileBackupsEnabling,
} from '../../../src/helpers.js'
import { isUnsupportedFault } from '../../../src/infra.js'

// On-demand backup lifecycle and restore-from-backup. Backups outlive their
// source table, so each backup is deleted explicitly in teardown.

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

describe('On-demand backups — lifecycle and restore', { tags: ['backups', 'control-plane', 'cloud-only'] }, () => {
  const source = uniqueTableName('bk_src')
  const backups: string[] = []
  const tables: string[] = [source]
  let supported = true

  async function waitForBackupAvailable(arn: string, timeoutMs = 60_000): Promise<void> {
    const start = Date.now()
    for (;;) {
      const d = await ddb.send(new DescribeBackupCommand({ BackupArn: arn }))
      if (d.BackupDescription?.BackupDetails?.BackupStatus === 'AVAILABLE') return
      if (Date.now() - start >= timeoutMs) throw new Error('backup not AVAILABLE in time')
      await sleep(2000)
    }
  }

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
    try {
      await ddb.send(new ListBackupsCommand({ TableName: source }))
    } catch (e) {
      if (isUnsupportedFault(e)) supported = false
      else throw e
    }
  }, 120_000)

  afterAll(async () => {
    for (const arn of backups) {
      await ddb.send(new DeleteBackupCommand({ BackupArn: arn })).catch(() => {})
    }
    // A restored table may still be CREATING (restore is slow) and cannot be
    // deleted until it settles, so wait it out before deleting.
    for (const t of tables) {
      try {
        await waitUntilActive(t, 540_000)
      } catch {
        // fall through to best-effort delete
      }
      await deleteTable(t)
    }
  }, 600_000)

  it('CreateBackup → DescribeBackup → ListBackups → DeleteBackup', async ({ skip }) => {
    if (!supported) return skip()
    const created = await retryWhileBackupsEnabling(() =>
      ddb.send(new CreateBackupCommand({ TableName: source, BackupName: 'conformance_backup' })),
    )
    const arn = created.BackupDetails!.BackupArn!
    expect(arn).toBeTruthy()
    backups.push(arn)

    await waitForBackupAvailable(arn)
    const desc = await ddb.send(new DescribeBackupCommand({ BackupArn: arn }))
    expect(desc.BackupDescription?.BackupDetails?.BackupStatus).toBe('AVAILABLE')

    const list = await ddb.send(new ListBackupsCommand({ TableName: source }))
    expect((list.BackupSummaries ?? []).map((b) => b.BackupArn)).toContain(arn)

    const del = await ddb.send(new DeleteBackupCommand({ BackupArn: arn }))
    expect(del.BackupDescription?.BackupDetails?.BackupArn).toBe(arn)
    backups.splice(backups.indexOf(arn), 1)
  }, 120_000)

  it('RestoreTableFromBackup initiates a restore into a new table', async ({ skip }) => {
    if (!supported) return skip()
    const created = await retryWhileBackupsEnabling(() =>
      ddb.send(new CreateBackupCommand({ TableName: source, BackupName: 'conformance_restore_src' })),
    )
    const arn = created.BackupDetails!.BackupArn!
    backups.push(arn)
    await waitForBackupAvailable(arn)

    const target = uniqueTableName('bk_restored')
    tables.push(target)
    // Restore is slow (minutes to ACTIVE), so assert the API accepts it and
    // reports the restore in progress; teardown waits it out before deleting.
    const res = await ddb.send(
      new RestoreTableFromBackupCommand({ TargetTableName: target, BackupArn: arn }),
    )
    expect(res.TableDescription?.TableName).toBe(target)
    expect(res.TableDescription?.RestoreSummary?.SourceBackupArn).toBe(arn)
    expect(res.TableDescription?.RestoreSummary?.RestoreInProgress).toBe(true)
  }, 120_000)

  it('DescribeBackup on a deleted backup throws BackupNotFoundException', async ({ skip }) => {
    if (!supported) return skip()
    const created = await retryWhileBackupsEnabling(() =>
      ddb.send(new CreateBackupCommand({ TableName: source, BackupName: 'conformance_gone' })),
    )
    const arn = created.BackupDetails!.BackupArn!
    await waitForBackupAvailable(arn)
    await ddb.send(new DeleteBackupCommand({ BackupArn: arn }))
    try {
      await ddb.send(new DescribeBackupCommand({ BackupArn: arn }))
      expect.unreachable('backup should be gone')
    } catch (e: unknown) {
      expect((e as DynamoDBServiceException).name).toBe('BackupNotFoundException')
    }
  }, 120_000)
})
