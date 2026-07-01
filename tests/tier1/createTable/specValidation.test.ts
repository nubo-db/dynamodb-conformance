import {
  CreateTableCommand,
  DynamoDBServiceException,
  type CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName } from '../../../src/helpers.js'

describe('CreateTable — index and stream spec validation', { tags: ['create-table', 'control-plane', 'negative-path'] }, () => {
  const expectRejected = async (input: CreateTableCommandInput) => {
    try {
      await ddb.send(new CreateTableCommand(input))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    }
  }

  it('rejects an INCLUDE GSI projection without NonKeyAttributes', () =>
    expectRejected({
      TableName: uniqueTableName('ct_inc_no_nka'),
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'g', AttributeType: 'S' },
      ],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'gsi1',
          KeySchema: [{ AttributeName: 'g', KeyType: 'HASH' }],
          Projection: { ProjectionType: 'INCLUDE' },
        },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    }))

  it('rejects a KEYS_ONLY GSI projection carrying NonKeyAttributes', () =>
    expectRejected({
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
    }))

  it('rejects a disabled stream carrying a StreamViewType', () =>
    expectRejected({
      TableName: uniqueTableName('ct_stream_off_vt'),
      AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      StreamSpecification: { StreamEnabled: false, StreamViewType: 'NEW_IMAGE' },
      BillingMode: 'PAY_PER_REQUEST',
    }))
})
