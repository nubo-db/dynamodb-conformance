import { CreateTableCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('CreateTable — exact error messages', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('missing TableName: exact error message', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: undefined as unknown as string,
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /TableName.*required|Member must not be null/,
    )
  })

  it('empty/short table name: "TableName must be at least 3 characters long"', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: 'ab',
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /TableName must be at least 3 characters long|Member must have length greater than or equal to 3/,
    )
  })

  it('duplicate attribute in KeySchema', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_dup_key'),
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'pk', KeyType: 'RANGE' },
          ],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Dd]uplicate|Invalid KeySchema|Both the Hash Key and the Range Key element in the KeySchema have the same Attribute/,
    )
  })

  it('more than 2 KeySchema elements', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_3keys'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'sk', AttributeType: 'S' },
            { AttributeName: 'extra', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'sk', KeyType: 'RANGE' },
            { AttributeName: 'extra', KeyType: 'RANGE' } as any,
          ],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Kk]ey[Ss]chema|Too many key schema elements|keySchema.*member must have length less than or equal to 2/i,
    )
  })

  it('invalid KeyType', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_bad_keytype'),
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
          KeySchema: [
            // @ts-expect-error -- testing invalid KeyType
            { AttributeName: 'pk', KeyType: 'INVALID' },
          ],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Kk]ey[Tt]ype|enum value set/,
    )
  })

  it('invalid AttributeType', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_bad_attrtype'),
          // @ts-expect-error -- testing invalid AttributeType
          AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'INVALID' }],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Aa]ttribute[Tt]ype|enum value set/,
    )
  })

  it('LSI without range key on base table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_lsi_no_range'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'lsiSk', AttributeType: 'S' },
          ],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          LocalSecondaryIndexes: [
            {
              IndexName: 'lsi1',
              KeySchema: [
                { AttributeName: 'pk', KeyType: 'HASH' },
                { AttributeName: 'lsiSk', KeyType: 'RANGE' },
              ],
              Projection: { ProjectionType: 'ALL' },
            },
          ],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Ll]ocal [Ss]econdary|Table KeySchema does not have a range key/,
    )
  })

  it('duplicate index names', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_dup_idx'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'sk', AttributeType: 'S' },
            { AttributeName: 'gsiPk1', AttributeType: 'S' },
            { AttributeName: 'gsiPk2', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'sk', KeyType: 'RANGE' },
          ],
          GlobalSecondaryIndexes: [
            {
              IndexName: 'sameIndex',
              KeySchema: [{ AttributeName: 'gsiPk1', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'ALL' },
              ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
            },
            {
              IndexName: 'sameIndex',
              KeySchema: [{ AttributeName: 'gsiPk2', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'ALL' },
              ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
            },
          ],
          ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
        }),
      ),
      'ValidationException',
      /[Dd]uplicate/,
    )
  })
})
