import { CreateTableCommand, DynamoDBServiceException } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName } from '../../../src/helpers.js'

// The INCLUDE-without-NonKeyAttributes and disabled-stream-with-StreamViewType
// rejections are already covered in createTable/basic.test.ts; this pins the
// remaining corner real AWS rejects.
describe('CreateTable — index and stream spec validation', { tags: ['create-table', 'control-plane', 'negative-path'] }, () => {
  it('rejects a KEYS_ONLY GSI projection carrying NonKeyAttributes', async () => {
    try {
      await ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_ko_nka'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'g', AttributeType: 'S' },
          ],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          GlobalSecondaryIndexes: [
            {
              IndexName: 'gsi1',
              KeySchema: [{ AttributeName: 'g', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'KEYS_ONLY', NonKeyAttributes: ['x'] },
            },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  })
})
