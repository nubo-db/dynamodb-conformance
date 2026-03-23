import {
  CreateTableCommand,
  DescribeTableCommand,
  TagResourceCommand,
  UntagResourceCommand,
  ListTagsOfResourceCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'

async function waitForTags(arn: string, expectedCount: number, timeoutMs = 10_000): Promise<any> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const res = await ddb.send(new ListTagsOfResourceCommand({ ResourceArn: arn }))
    if ((res.Tags?.length ?? 0) >= expectedCount) return res
    await new Promise(r => setTimeout(r, 500))
  }
  throw new Error(`Timeout waiting for ${expectedCount} tags`)
}

async function waitForTagCount(arn: string, expectedCount: number, timeoutMs = 10_000): Promise<any> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const res = await ddb.send(new ListTagsOfResourceCommand({ ResourceArn: arn }))
    if ((res.Tags?.length ?? 0) === expectedCount) return res
    await new Promise(r => setTimeout(r, 500))
  }
  throw new Error(`Timeout waiting for exactly ${expectedCount} tags`)
}

describe('Tags — basic', () => {
  const tableName = uniqueTableName('tags')
  let tableArn: string

  beforeAll(async () => {
    await ddb.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(tableName)

    const desc = await ddb.send(
      new DescribeTableCommand({ TableName: tableName }),
    )
    tableArn = desc.Table!.TableArn!
  })

  afterAll(async () => {
    await deleteTable(tableName)
  })

  it('adds tags to a table', async () => {
    const res = await ddb.send(
      new TagResourceCommand({
        ResourceArn: tableArn,
        Tags: [
          { Key: 'env', Value: 'test' },
          { Key: 'project', Value: 'conformity' },
        ],
      }),
    )

    // TagResource returns no meaningful body — success is no error
    expect(res.$metadata.httpStatusCode).toBe(200)
  })

  it('lists tags and verifies they match what was added', async () => {
    const res = await waitForTags(tableArn, 2)

    expect(res.Tags).toBeDefined()
    const tags = res.Tags!
    expect(tags).toEqual(
      expect.arrayContaining([
        { Key: 'env', Value: 'test' },
        { Key: 'project', Value: 'conformity' },
      ]),
    )
  })

  it('adds additional tags and verifies all tags are present', async () => {
    await ddb.send(
      new TagResourceCommand({
        ResourceArn: tableArn,
        Tags: [{ Key: 'team', Value: 'platform' }],
      }),
    )

    const res = await waitForTags(tableArn, 3)

    const tags = res.Tags!
    expect(tags).toEqual(
      expect.arrayContaining([
        { Key: 'env', Value: 'test' },
        { Key: 'project', Value: 'conformity' },
        { Key: 'team', Value: 'platform' },
      ]),
    )
  })

  it('removes specific tag keys with UntagResource', async () => {
    await ddb.send(
      new UntagResourceCommand({
        ResourceArn: tableArn,
        TagKeys: ['team'],
      }),
    )

    const res = await waitForTagCount(tableArn, 2)

    const tags = res.Tags!
    const tagKeys = tags.map((t: any) => t.Key)
    expect(tagKeys).not.toContain('team')
    expect(tagKeys).toContain('env')
    expect(tagKeys).toContain('project')
  })

  it('verifies removed tags are gone after untag', async () => {
    // Remove the remaining 'project' tag
    await ddb.send(
      new UntagResourceCommand({
        ResourceArn: tableArn,
        TagKeys: ['project'],
      }),
    )

    const res = await waitForTagCount(tableArn, 1)

    const tags = res.Tags!
    const tagKeys = tags.map((t: any) => t.Key)
    expect(tagKeys).not.toContain('project')
    expect(tagKeys).toContain('env')
  })

  it('overwrites an existing tag with the same key but different value', async () => {
    await ddb.send(
      new TagResourceCommand({
        ResourceArn: tableArn,
        Tags: [{ Key: 'env', Value: 'production' }],
      }),
    )

    // Wait for the new value to propagate (tags are eventually consistent)
    const start = Date.now()
    let envValue = ''
    while (Date.now() - start < 15_000) {
      const res = await ddb.send(new ListTagsOfResourceCommand({ ResourceArn: tableArn }))
      const envTag = res.Tags?.find((t: any) => t.Key === 'env')
      if (envTag?.Value === 'production') {
        envValue = envTag.Value
        break
      }
      await new Promise(r => setTimeout(r, 500))
    }
    expect(envValue).toBe('production')
  })
})

describe('Tags — validation', () => {
  it('rejects TagResource with an invalid ARN format', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TagResourceCommand({
            ResourceArn: 'not-a-valid-arn',
            Tags: [{ Key: 'env', Value: 'test' }],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects ListTagsOfResource with a non-existent ARN', async () => {
    try {
      await ddb.send(
        new ListTagsOfResourceCommand({
          ResourceArn:
            'arn:aws:dynamodb:us-east-1:000000000000:table/nonexistent_table_xyz',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeDefined()
      expect((e as Error).name).toBe('AccessDeniedException')
    }
  })
})
