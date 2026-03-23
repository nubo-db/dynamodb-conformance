import {
  CreateTableCommand,
  DescribeTimeToLiveCommand,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'

/** Create a simple hash-only table for TTL tests */
async function createSimpleTable(name: string): Promise<void> {
  await ddb.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
    }),
  )
  await waitUntilActive(name)
}

describe('TTL — basic', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('enables TTL on a table', async () => {
    const name = uniqueTableName('ttl_enable')
    tablesToCleanup.push(name)
    await createSimpleTable(name)

    const res = await ddb.send(
      new UpdateTimeToLiveCommand({
        TableName: name,
        TimeToLiveSpecification: {
          Enabled: true,
          AttributeName: 'ttl',
        },
      }),
    )

    expect(res.TimeToLiveSpecification).toBeDefined()
    expect(res.TimeToLiveSpecification!.Enabled).toBe(true)
    expect(res.TimeToLiveSpecification!.AttributeName).toBe('ttl')
  })

  it('DescribeTimeToLive returns ENABLED status and correct attribute name after enabling', async () => {
    const name = uniqueTableName('ttl_describe_enabled')
    tablesToCleanup.push(name)
    await createSimpleTable(name)

    await ddb.send(
      new UpdateTimeToLiveCommand({
        TableName: name,
        TimeToLiveSpecification: {
          Enabled: true,
          AttributeName: 'ttl',
        },
      }),
    )

    const desc = await ddb.send(
      new DescribeTimeToLiveCommand({ TableName: name }),
    )

    expect(desc.TimeToLiveDescription).toBeDefined()
    expect(['ENABLED', 'ENABLING']).toContain(
      desc.TimeToLiveDescription!.TimeToLiveStatus,
    )
    expect(desc.TimeToLiveDescription!.AttributeName).toBe('ttl')
  })

  it('DescribeTimeToLive returns DISABLED on a table with no TTL configured', async () => {
    const name = uniqueTableName('ttl_describe_disabled')
    tablesToCleanup.push(name)
    await createSimpleTable(name)

    const desc = await ddb.send(
      new DescribeTimeToLiveCommand({ TableName: name }),
    )

    expect(desc.TimeToLiveDescription).toBeDefined()
    expect(desc.TimeToLiveDescription!.TimeToLiveStatus).toBe('DISABLED')
  })

  it('enables TTL with a different attribute name', async () => {
    const name = uniqueTableName('ttl_custom_attr')
    tablesToCleanup.push(name)
    await createSimpleTable(name)

    const res = await ddb.send(
      new UpdateTimeToLiveCommand({
        TableName: name,
        TimeToLiveSpecification: {
          Enabled: true,
          AttributeName: 'expiresAt',
        },
      }),
    )

    expect(res.TimeToLiveSpecification!.AttributeName).toBe('expiresAt')
    expect(res.TimeToLiveSpecification!.Enabled).toBe(true)

    const desc = await ddb.send(
      new DescribeTimeToLiveCommand({ TableName: name }),
    )
    expect(desc.TimeToLiveDescription!.AttributeName).toBe('expiresAt')
  })
})

describe('TTL — validation', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('rejects empty attribute name', async () => {
    const name = uniqueTableName('ttl_empty_attr')
    tablesToCleanup.push(name)
    await createSimpleTable(name)

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTimeToLiveCommand({
            TableName: name,
            TimeToLiveSpecification: {
              Enabled: true,
              AttributeName: '',
            },
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects UpdateTimeToLive on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTimeToLiveCommand({
            TableName: 'nonexistent_table_that_does_not_exist',
            TimeToLiveSpecification: {
              Enabled: true,
              AttributeName: 'ttl',
            },
          }),
        ),
      'ResourceNotFoundException',
    )
  })
})
