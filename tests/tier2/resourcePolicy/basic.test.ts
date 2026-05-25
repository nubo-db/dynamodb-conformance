import {
  CreateTableCommand,
  DescribeTableCommand,
  PutResourcePolicyCommand,
  GetResourcePolicyCommand,
  DeleteResourcePolicyCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'
import { isUnsupportedFault } from '../../../src/infra.js'

// Resource-based policies on a table. The principal account ID is derived from
// the table ARN at runtime (never hardcoded); the widened account-ID guard
// sanitises any ARN/principal that reaches a result file.

describe('Resource policies — Put/Get/Delete', () => {
  const table = uniqueTableName('respolicy')
  let arn = ''
  let supported = true

  function policyFor(resourceArn: string, action = 'dynamodb:GetItem'): string {
    const account = resourceArn.split(':')[4]
    return JSON.stringify({
      Version: '2012-10-17',
      Statement: [
        {
          Sid: 'conformance',
          Effect: 'Allow',
          Principal: { AWS: `arn:aws:iam::${account}:root` },
          Action: action,
          Resource: resourceArn,
        },
      ],
    })
  }

  beforeAll(async () => {
    await ddb.send(
      new CreateTableCommand({
        TableName: table,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(table)
    arn = (await ddb.send(new DescribeTableCommand({ TableName: table }))).Table!.TableArn!
    try {
      await ddb.send(new GetResourcePolicyCommand({ ResourceArn: arn }))
    } catch (e) {
      // PolicyNotFoundException means the operation is supported (no policy yet).
      if (isUnsupportedFault(e)) supported = false
    }
  }, 120_000)

  afterAll(async () => {
    await deleteTable(table) // deleting the table removes any attached policy
  })

  it('GetResourcePolicy on a table with no policy throws PolicyNotFoundException', async ({ skip }) => {
    if (!supported) return skip()
    try {
      await ddb.send(new GetResourcePolicyCommand({ ResourceArn: arn }))
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeInstanceOf(DynamoDBServiceException)
      expect((e as DynamoDBServiceException).name).toBe('PolicyNotFoundException')
    }
  })

  // Resource policies are eventually consistent (poll rather than read once) and
  // their writes are throttled to roughly once per several seconds (retry).
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

  async function retryOnThrottle<T>(fn: () => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await fn()
      } catch (e: unknown) {
        if (
          e instanceof DynamoDBServiceException &&
          e.name === 'ThrottlingException' &&
          attempt < 3
        ) {
          await sleep(16_000)
          continue
        }
        throw e
      }
    }
  }

  async function waitForPolicyPresent(timeoutMs = 30_000) {
    const start = Date.now()
    for (;;) {
      try {
        return await ddb.send(new GetResourcePolicyCommand({ ResourceArn: arn }))
      } catch (e: unknown) {
        const stillMissing =
          e instanceof DynamoDBServiceException && e.name === 'PolicyNotFoundException'
        if (stillMissing && Date.now() - start < timeoutMs) {
          await sleep(1000)
          continue
        }
        throw e
      }
    }
  }

  async function waitForPolicyAbsent(timeoutMs = 30_000) {
    const start = Date.now()
    for (;;) {
      try {
        await ddb.send(new GetResourcePolicyCommand({ ResourceArn: arn }))
      } catch (e: unknown) {
        if (e instanceof DynamoDBServiceException && e.name === 'PolicyNotFoundException') return
        throw e
      }
      if (Date.now() - start >= timeoutMs) throw new Error('policy still present after timeout')
      await sleep(1000)
    }
  }

  it('Put then Get round-trips the policy, and Delete removes it', async ({ skip }) => {
    if (!supported) return skip()
    const put = await retryOnThrottle(() =>
      ddb.send(new PutResourcePolicyCommand({ ResourceArn: arn, Policy: policyFor(arn) })),
    )
    expect(put.RevisionId).toBeTruthy()

    const got = await waitForPolicyPresent()
    expect(got.Policy).toBeTruthy()
    expect(got.RevisionId).toBe(put.RevisionId)

    await retryOnThrottle(() =>
      ddb.send(new DeleteResourcePolicyCommand({ ResourceArn: arn })),
    )
    await waitForPolicyAbsent()
  })
})
