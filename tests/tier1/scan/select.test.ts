import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('Scan — Select COUNT', { tags: ['scan', 'data-plane'] }, () => {
  const items = [
    { pk: { S: 'scan-select-0' }, category: { S: 'alpha' } },
    { pk: { S: 'scan-select-1' }, category: { S: 'beta' } },
    { pk: { S: 'scan-select-2' }, category: { S: 'alpha' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('Select COUNT returns count without items on Scan', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBeGreaterThan(0)
    expect(result.Items).toBeUndefined()
  })

  it('Select COUNT with FilterExpression on Scan', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#c = :cat AND begins_with(pk, :prefix)',
        ExpressionAttributeNames: { '#c': 'category' },
        ExpressionAttributeValues: {
          ':cat': { S: 'alpha' },
          ':prefix': { S: 'scan-select-' },
        },
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBe(2)
    expect(result.ScannedCount).toBeGreaterThanOrEqual(3)
    expect(result.Items).toBeUndefined()
  })
})

// ProjectionExpression requires SPECIFIC_ATTRIBUTES; ALL_PROJECTED_ATTRIBUTES requires an IndexName.
// AWS keeps the word "Querying" in the second message even on a Scan.
describe('Scan — Select / ProjectionExpression rejections', { tags: ['scan', 'data-plane'] }, () => {
  it('Select ALL_ATTRIBUTES with ProjectionExpression is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            Select: 'ALL_ATTRIBUTES',
            ProjectionExpression: 'pk',
          }),
        ),
      'ValidationException',
      'Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES',
    )
  })

  it('Select ALL_PROJECTED_ATTRIBUTES without an IndexName is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            Select: 'ALL_PROJECTED_ATTRIBUTES',
          }),
        ),
      'ValidationException',
      'ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName',
    )
  })

  it('Select COUNT with ProjectionExpression is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            Select: 'COUNT',
            ProjectionExpression: 'pk',
          }),
        ),
      'ValidationException',
      'Cannot specify the ProjectionExpression when choosing to get only the Count',
    )
  })

  // Both rules broken at once; AWS reports the ProjectionExpression one.
  it('Select ALL_PROJECTED_ATTRIBUTES with ProjectionExpression and no IndexName is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            Select: 'ALL_PROJECTED_ATTRIBUTES',
            ProjectionExpression: 'pk',
          }),
        ),
      'ValidationException',
      'Cannot specify the ProjectionExpression when choosing to get ALL_PROJECTED_ATTRIBUTES',
    )
  })
})
