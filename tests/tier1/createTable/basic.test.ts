import {
  CreateTableCommand,
  DescribeTableCommand,
  ListTablesCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
  hashTableDef,
} from '../../../src/helpers.js'

describe('CreateTable — basic', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('creates a hash-only table with PROVISIONED billing', async () => {
    const name = uniqueTableName('ct_hash')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const table = desc.Table!
    expect(table.TableStatus).toBe('ACTIVE')
    expect(table.TableName).toBe(name)
    expect(table.KeySchema).toEqual([
      { AttributeName: 'pk', KeyType: 'HASH' },
    ])
    expect(table.AttributeDefinitions).toEqual([
      { AttributeName: 'pk', AttributeType: 'S' },
    ])
  })

  it('creates a composite key table', async () => {
    const name = uniqueTableName('ct_composite')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'sk', AttributeType: 'N' },
        ],
        KeySchema: [
          { AttributeName: 'pk', KeyType: 'HASH' },
          { AttributeName: 'sk', KeyType: 'RANGE' },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.KeySchema).toEqual([
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'sk', KeyType: 'RANGE' },
    ])
  })

  it('creates a table with PAY_PER_REQUEST billing', async () => {
    const name = uniqueTableName('ct_ondemand')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.BillingModeSummary?.BillingMode).toBe('PAY_PER_REQUEST')
  })

  it('returns the table in ListTables after creation', async () => {
    const name = uniqueTableName('ct_list')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const list = await ddb.send(new ListTablesCommand({}))
    expect(list.TableNames).toContain(name)
  })
})

describe('CreateTable — validation', () => {
  it('rejects a table name shorter than 3 characters', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: 'ab',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects a table name longer than 255 characters', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: 'a'.repeat(256),
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects a table name with invalid characters', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: 'invalid;name',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects duplicate table creation with ResourceInUseException', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: hashTableDef.name,
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ResourceInUseException',
    )
  })

  it('rejects missing KeySchema', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_nokey'),
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects unused AttributeDefinitions', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_unused_attr'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'extra', AttributeType: 'N' },
          ],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })
})
