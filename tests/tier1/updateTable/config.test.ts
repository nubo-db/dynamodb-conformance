import {
  CreateTableCommand,
  DescribeTableCommand,
  UpdateTableCommand,
  DeleteTableCommand,
  type CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'

// UpdateTable mutations of the configuration parameters, plus the behaviour that
// matters most: deletion protection actually blocks DeleteTable. Characterised
// against real AWS in the U2 gap map.

describe('UpdateTable — configuration parameters', { tags: ['update-table', 'control-plane'] }, () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  async function createTable(extra: Partial<CreateTableCommandInput> = {}): Promise<string> {
    const name = uniqueTableName('ut_config')
    tablesToCleanup.push(name)
    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        ...extra,
      }),
    )
    await waitUntilActive(name)
    return name
  }

  it('UpdateTable enables DeletionProtectionEnabled', async () => {
    // Only one deletion-protection change here: AWS throttles changes to once
    // per 15s. Cleanup disables it via deleteTable, which absorbs that throttle.
    const name = await createTable()
    await ddb.send(
      new UpdateTableCommand({ TableName: name, DeletionProtectionEnabled: true }),
    )
    await waitUntilActive(name)
    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table?.DeletionProtectionEnabled).toBe(true)
  })

  it('UpdateTable changes TableClass', async () => {
    const name = await createTable()
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        TableClass: 'STANDARD_INFREQUENT_ACCESS',
      }),
    )
    await waitUntilActive(name)
    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table?.TableClassSummary?.TableClass).toBe('STANDARD_INFREQUENT_ACCESS')
  })

  it('UpdateTable changes OnDemandThroughput', async () => {
    const name = await createTable({
      OnDemandThroughput: { MaxReadRequestUnits: 10, MaxWriteRequestUnits: 5 },
    })
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        OnDemandThroughput: { MaxReadRequestUnits: 20, MaxWriteRequestUnits: 15 },
      }),
    )
    await waitUntilActive(name)
    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table?.OnDemandThroughput?.MaxReadRequestUnits).toBe(20)
    expect(desc.Table?.OnDemandThroughput?.MaxWriteRequestUnits).toBe(15)
  })
})

describe('DeleteTable — deletion protection enforcement', { tags: ['delete-table', 'control-plane'] }, () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('rejects DeleteTable while deletion protection is enabled', async () => {
    const name = uniqueTableName('dp_block')
    tablesToCleanup.push(name)
    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        DeletionProtectionEnabled: true,
      }),
    )
    await waitUntilActive(name)

    await expectDynamoError(
      () => ddb.send(new DeleteTableCommand({ TableName: name })),
      'ValidationException',
      'protected against deletion',
    )
  })

  it('allows DeleteTable after protection is disabled', async () => {
    const name = uniqueTableName('dp_allow')
    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        DeletionProtectionEnabled: true,
      }),
    )
    await waitUntilActive(name)
    await ddb.send(
      new UpdateTableCommand({ TableName: name, DeletionProtectionEnabled: false }),
    )
    await waitUntilActive(name)
    // Should now succeed; no error means the gate was the flag, not unconditional.
    await ddb.send(new DeleteTableCommand({ TableName: name }))
  })
})
