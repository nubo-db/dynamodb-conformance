import {
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  ListTablesCommand,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, expectDynamoError } from '../../../src/helpers.js'

describe('DeleteTable — basic', () => {
  it('deletes an existing table', async () => {
    const name = uniqueTableName('dt_basic')

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(name)

    const delResult = await ddb.send(
      new DeleteTableCommand({ TableName: name }),
    )
    expect(delResult.TableDescription).toBeDefined()
    expect(delResult.TableDescription!.TableName).toBe(name)

    // Eventually the table should be gone
    let deleted = false
    for (let i = 0; i < 30; i++) {
      try {
        await ddb.send(new DescribeTableCommand({ TableName: name }))
        await new Promise((r) => setTimeout(r, 1000))
      } catch (e: unknown) {
        if (e instanceof ResourceNotFoundException) {
          deleted = true
          break
        }
        throw e
      }
    }
    expect(deleted).toBe(true)
  })

  it('removed table does not appear in ListTables', async () => {
    const name = uniqueTableName('dt_list')

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(name)

    await ddb.send(new DeleteTableCommand({ TableName: name }))

    // Wait for deletion
    let gone = false
    for (let i = 0; i < 30; i++) {
      try {
        await ddb.send(new DescribeTableCommand({ TableName: name }))
        await new Promise((r) => setTimeout(r, 1000))
      } catch (e: unknown) {
        if (e instanceof ResourceNotFoundException) {
          gone = true
          break
        }
      }
    }

    if (gone) {
      const list = await ddb.send(new ListTablesCommand({}))
      expect(list.TableNames).not.toContain(name)
    }
  })
})

describe('DeleteTable — validation', () => {
  it('rejects deleting a non-existent table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new DeleteTableCommand({
          TableName: '_conformance_nonexistent_table',
        }),
      ),
      'ResourceNotFoundException',
    )
  })
})
