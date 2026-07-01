import { ScanCommand, DynamoDBServiceException } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef } from '../../../src/helpers.js'

describe('Scan — FilterExpression operand validation order', { tags: ['scan', 'data-plane', 'negative-path'] }, () => {
  it('rejects a non-string begins_with operand even when nothing matches', async () => {
    // The operand-type check fires before row evaluation, so a scan whose filter
    // matches no rows still rejects a wrong-typed begins_with operand rather
    // than returning an empty result.
    try {
      await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          FilterExpression: 'begins_with(#p, :n)',
          ExpressionAttributeNames: { '#p': 'no_such_attr_xyz' },
          ExpressionAttributeValues: { ':n': { N: '5' } },
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })
})
