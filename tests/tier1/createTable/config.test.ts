import {
  CreateTableCommand,
  DescribeTableCommand,
  type CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'

// Table-configuration parameters set at create time, asserted via a
// DescribeTable round-trip. Characterised against real AWS in the U2 gap map.

describe('CreateTable — configuration parameters', { tags: ['create-table', 'control-plane'] }, () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  async function createAndDescribe(extra: Partial<CreateTableCommandInput>) {
    const name = uniqueTableName('ct_config')
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
    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    return { name, table: desc.Table! }
  }

  it('DeletionProtectionEnabled round-trips', async () => {
    const { table } = await createAndDescribe({ DeletionProtectionEnabled: true })
    expect(table.DeletionProtectionEnabled).toBe(true)
  })

  it('omitting DeletionProtectionEnabled defaults to disabled', async () => {
    const { table } = await createAndDescribe({})
    expect(table.DeletionProtectionEnabled ?? false).toBe(false)
  })

  it('TableClass STANDARD_INFREQUENT_ACCESS round-trips', async () => {
    const { table } = await createAndDescribe({
      TableClass: 'STANDARD_INFREQUENT_ACCESS',
    })
    expect(table.TableClassSummary?.TableClass).toBe('STANDARD_INFREQUENT_ACCESS')
  })

  it('omitting TableClass defaults to STANDARD', async () => {
    const { table } = await createAndDescribe({})
    // STANDARD may be reported explicitly or by omission.
    expect(table.TableClassSummary?.TableClass ?? 'STANDARD').toBe('STANDARD')
  })

  it('SSESpecification with the AWS-managed key round-trips', async () => {
    const { table } = await createAndDescribe({ SSESpecification: { Enabled: true } })
    expect(table.SSEDescription?.Status).toBe('ENABLED')
    expect(table.SSEDescription?.SSEType).toBe('KMS')
    // The KMS ARN carries the account ID, so assert presence/shape, not value.
    expect(table.SSEDescription?.KMSMasterKeyArn).toMatch(/^arn:aws:kms:/)
  })

  it('omitting SSESpecification leaves SSEDescription absent (default AWS-owned key)', async () => {
    const { table } = await createAndDescribe({})
    expect(table.SSEDescription).toBeUndefined()
  })

  it('OnDemandThroughput round-trips on a PAY_PER_REQUEST table', async () => {
    const { table } = await createAndDescribe({
      OnDemandThroughput: { MaxReadRequestUnits: 10, MaxWriteRequestUnits: 5 },
    })
    expect(table.OnDemandThroughput?.MaxReadRequestUnits).toBe(10)
    expect(table.OnDemandThroughput?.MaxWriteRequestUnits).toBe(5)
  })
})
