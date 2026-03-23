import { QueryCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('Query — exact error messages', () => {
  it('missing hash key in KeyConditionExpression', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'sk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      ),
      'ValidationException',
      /[Kk]ey condition|Query condition missed key schema element/i,
    )
  })

  it('non-key attribute in KeyConditionExpression', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'attr1 = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      ),
      'ValidationException',
      /[Kk]ey.*schema/i,
    )
  })

  it('unused ExpressionAttributeNames', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          ExpressionAttributeNames: { '#unused': 'someattr' },
        }),
      ),
      'ValidationException',
      'Value provided in ExpressionAttributeNames unused in expressions',
    )
  })

  it('invalid Select value', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          // @ts-expect-error -- testing invalid Select
          Select: 'INVALID_VALUE',
        }),
      ),
      'ValidationException',
      /Member must satisfy enum value set|Value .* at 'select' failed to satisfy/,
    )
  })

  it('ConsistentRead on GSI', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          IndexName: 'gsi1',
          KeyConditionExpression: '#hk = :v',
          ExpressionAttributeNames: { '#hk': 'lsi1sk' },
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          ConsistentRead: true,
        }),
      ),
      'ValidationException',
      'Consistent reads are not supported on global secondary indexes',
    )
  })

  it('Limit of 0', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :v',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
          Limit: 0,
        }),
      ),
      'ValidationException',
      /[Ll]imit/,
    )
  })

  it('empty KeyConditionExpression', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: '',
          ExpressionAttributeValues: { ':v': { S: 'val' } },
        }),
      ),
      'ValidationException',
      /expression/i,
    )
  })

  it('filter references undefined ExpressionAttributeNames', async () => {
    await expectDynamoError(
      () => ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :pk',
          FilterExpression: '#missing = :fv',
          ExpressionAttributeValues: { ':pk': { S: 'val' }, ':fv': { S: 'x' } },
        }),
      ),
      'ValidationException',
      /ExpressionAttributeNames|substitution.*not found|Invalid FilterExpression/,
    )
  })
})
