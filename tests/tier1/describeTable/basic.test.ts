import { DescribeTableCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('DescribeTable — basic', () => {
  it('returns table metadata for a hash-only table', async () => {
    const result = await ddb.send(
      new DescribeTableCommand({ TableName: hashTableDef.name }),
    )
    const table = result.Table!

    expect(table.TableName).toBe(hashTableDef.name)
    expect(table.TableStatus).toBe('ACTIVE')
    expect(table.KeySchema).toEqual([
      { AttributeName: 'pk', KeyType: 'HASH' },
    ])
    expect(table.AttributeDefinitions).toEqual([
      { AttributeName: 'pk', AttributeType: 'S' },
    ])
    expect(table.TableArn).toBeDefined()
    expect(table.CreationDateTime).toBeDefined()
    expect(table.ItemCount).toBeDefined()
    expect(table.TableSizeBytes).toBeDefined()
  })

  it('returns table metadata for a composite table with indexes', async () => {
    const result = await ddb.send(
      new DescribeTableCommand({ TableName: compositeTableDef.name }),
    )
    const table = result.Table!

    expect(table.TableName).toBe(compositeTableDef.name)
    expect(table.KeySchema).toHaveLength(2)

    // LSIs
    expect(table.LocalSecondaryIndexes).toBeDefined()
    expect(table.LocalSecondaryIndexes).toHaveLength(2)

    // GSIs
    expect(table.GlobalSecondaryIndexes).toBeDefined()
    expect(table.GlobalSecondaryIndexes).toHaveLength(2)

    // Each GSI should have IndexStatus
    for (const gsi of table.GlobalSecondaryIndexes!) {
      expect(gsi.IndexName).toBeDefined()
      expect(gsi.IndexStatus).toBe('ACTIVE')
      expect(gsi.KeySchema).toBeDefined()
      expect(gsi.Projection).toBeDefined()
    }
  })
})

describe('DescribeTable — validation', () => {
  it('returns ResourceNotFoundException for non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new DescribeTableCommand({
          TableName: 'this_table_does_not_exist_xyz',
        }),
      ),
      'ResourceNotFoundException',
    )
  })
})
