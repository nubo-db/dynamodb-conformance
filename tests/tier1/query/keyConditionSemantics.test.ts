import {
  QueryCommand,
  PutItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — KeyConditionExpression semantics', { tags: ['query', 'data-plane'] }, () => {
  const pk = 'kce-sem'
  afterEach(async () => {
    await cleanupItems(compositeTableDef.name, [{ pk: { S: pk }, sk: { S: 'm' } }])
  })

  it('accepts a reversed-operand sort-key comparison', async () => {
    // Real AWS accepts the value on the left of the comparator (:lo <= #sk),
    // equivalent to #sk >= :lo, not only attribute-on-left.
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: pk }, sk: { S: 'm' } },
      }),
    )
    const res = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk AND :lo <= #sk',
        ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
        ExpressionAttributeValues: { ':pk': { S: pk }, ':lo': { S: 'a' } },
      }),
    )
    expect(res.Items).toHaveLength(1)
    expect(res.Items![0].sk.S).toBe('m')
  })

  it('rejects a nested path on a key attribute', async () => {
    // Real AWS rejects a document-path reference on a key attribute in a
    // KeyConditionExpression.
    try {
      await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: '#pk = :pk AND #sk.foo = :v',
          ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
          ExpressionAttributeValues: { ':pk': { S: pk }, ':v': { S: 'x' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain(
        'KeyConditionExpressions cannot have conditions on nested attributes',
      )
    }
  })
})
