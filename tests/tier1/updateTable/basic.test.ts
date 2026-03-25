import {
  CreateTableCommand,
  UpdateTableCommand,
  DescribeTableCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'

/** Helper to create a simple hash-key table with the given billing mode */
async function createSimpleTable(
  name: string,
  billingMode: 'PROVISIONED' | 'PAY_PER_REQUEST' = 'PROVISIONED',
): Promise<void> {
  await ddb.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      ...(billingMode === 'PAY_PER_REQUEST'
        ? { BillingMode: 'PAY_PER_REQUEST' }
        : {
            ProvisionedThroughput: {
              ReadCapacityUnits: 5,
              WriteCapacityUnits: 5,
            },
          }),
    }),
  )
  await waitUntilActive(name)
}

describe('UpdateTable — basic', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('changes provisioned throughput on a PROVISIONED table', async () => {
    const name = uniqueTableName('ut_throughput')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PROVISIONED')

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10,
        },
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const throughput = desc.Table!.ProvisionedThroughput!
    expect(throughput.ReadCapacityUnits).toBe(10)
    expect(throughput.WriteCapacityUnits).toBe(10)
  })

  it('rejects PROVISIONED to PROVISIONED no-op with same throughput values', async () => {
    const name = uniqueTableName('ut_prov_noop')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PROVISIONED')

    await expectDynamoError(
      () => ddb.send(
        new UpdateTableCommand({
          TableName: name,
          BillingMode: 'PROVISIONED',
          ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5,
          },
        }),
      ),
      'ValidationException',
    )
  })

  it('accepts PAY_PER_REQUEST to PAY_PER_REQUEST no-op (same billing mode)', async () => {
    const name = uniqueTableName('ut_odr_noop')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PAY_PER_REQUEST')

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.BillingModeSummary?.BillingMode).toBe('PAY_PER_REQUEST')
  })

  it('switches billing mode from PROVISIONED to PAY_PER_REQUEST', { timeout: 600_000 }, async () => {
    const name = uniqueTableName('ut_to_ondemand')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PROVISIONED')

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name, 300_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.BillingModeSummary?.BillingMode).toBe('PAY_PER_REQUEST')
  })

  it('switches billing mode from PAY_PER_REQUEST to PROVISIONED', { timeout: 600_000 }, async () => {
    const name = uniqueTableName('ut_to_prov')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PAY_PER_REQUEST')

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        BillingMode: 'PROVISIONED',
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      }),
    )

    await waitUntilActive(name, 300_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const throughput = desc.Table!.ProvisionedThroughput!
    expect(throughput.ReadCapacityUnits).toBe(5)
    expect(throughput.WriteCapacityUnits).toBe(5)
  })

  it('DescribeTable reflects updated throughput values', async () => {
    const name = uniqueTableName('ut_desc_tp')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PROVISIONED')

    // Verify initial throughput
    const before = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(before.Table!.ProvisionedThroughput!.ReadCapacityUnits).toBe(5)
    expect(before.Table!.ProvisionedThroughput!.WriteCapacityUnits).toBe(5)

    // Update throughput
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        ProvisionedThroughput: {
          ReadCapacityUnits: 20,
          WriteCapacityUnits: 15,
        },
      }),
    )

    await waitUntilActive(name)

    // Verify updated throughput
    const after = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(after.Table!.ProvisionedThroughput!.ReadCapacityUnits).toBe(20)
    expect(after.Table!.ProvisionedThroughput!.WriteCapacityUnits).toBe(15)
  })

  it('DescribeTable reflects updated billing mode', { timeout: 600_000 }, async () => {
    const name = uniqueTableName('ut_desc_bm')
    tablesToCleanup.push(name)
    await createSimpleTable(name, 'PROVISIONED')

    // Switch to on-demand
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name, 300_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.BillingModeSummary?.BillingMode).toBe('PAY_PER_REQUEST')
    expect(desc.Table!.TableStatus).toBe('ACTIVE')
  })
})

describe('UpdateTable — validation', () => {
  it('rejects UpdateTable on a non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: '_conformance_nonexistent_table',
            ProvisionedThroughput: {
              ReadCapacityUnits: 10,
              WriteCapacityUnits: 10,
            },
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('rejects invalid throughput values (0 or negative)', async () => {
    const name = uniqueTableName('ut_invalid_tp')
    const tablesToCleanup: string[] = []
    tablesToCleanup.push(name)

    await createSimpleTable(name, 'PROVISIONED')

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: name,
            ProvisionedThroughput: {
              ReadCapacityUnits: 0,
              WriteCapacityUnits: 5,
            },
          }),
        ),
      'ValidationException',
    )

    // Cleanup
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('rejects PAY_PER_REQUEST with ProvisionedThroughput specified', async () => {
    const name = uniqueTableName('ut_ppr_with_tp')
    const tablesToCleanup: string[] = []
    tablesToCleanup.push(name)

    await createSimpleTable(name, 'PROVISIONED')

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: name,
            BillingMode: 'PAY_PER_REQUEST',
            ProvisionedThroughput: {
              ReadCapacityUnits: 5,
              WriteCapacityUnits: 5,
            },
          }),
        ),
      'ValidationException',
    )

    // Cleanup
    await Promise.all(tablesToCleanup.map(deleteTable))
  })
})
