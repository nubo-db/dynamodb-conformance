import {
  ExecuteStatementCommand,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'

describe('ExecuteStatement — PartiQL', { tags: ['partiql', 'data-plane'] }, () => {
  let supported = true

  const keysToCleanup: Record<string, { S: string }>[] = []
  const compositeKeysToCleanup: Record<string, AttributeValue>[] = []

  beforeAll(async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-canary'`,
      }))
    } catch (e: unknown) {
      if (e instanceof Error && (e.name === 'UnknownOperationException' || e.name === 'UnrecognizedClientException')) {
        supported = false
      }
    }
  })

  beforeEach(({ skip }) => { if (!supported) skip() })

  afterAll(async () => {
    if (keysToCleanup.length > 0) {
      await cleanupItems(hashTableDef.name, keysToCleanup)
    }
    if (compositeKeysToCleanup.length > 0) {
      await cleanupItems(compositeTableDef.name, compositeKeysToCleanup)
    }
  })

  it('INSERTs a new item', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-insert-1' } })

    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'partiql-insert-1', 'data': 'inserted'}`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-insert-1' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeDefined()
    expect(result.Item!.data.S).toBe('inserted')
  })

  it('SELECTs an item by primary key', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-select-1' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-select-1' }, data: { S: 'selectme' } },
    }))

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-select-1'`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('partiql-select-1')
    expect(result.Items![0].data.S).toBe('selectme')
  })

  it('SELECTs with WHERE clause using comparison', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-cmp-1' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-cmp-1' }, age: { N: '30' } },
    }))

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-cmp-1' AND age > 20`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].age.N).toBe('30')
  })

  it('UPDATEs an existing item', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-update-1' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-update-1' }, data: { S: 'before' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `UPDATE "${hashTableDef.name}" SET data = 'after' WHERE pk = 'partiql-update-1'`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-update-1' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeDefined()
    expect(result.Item!.data.S).toBe('after')
  })

  it('DELETEs an item', async () => {
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-delete-1' }, data: { S: 'gone' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = 'partiql-delete-1'`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-delete-1' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeUndefined()
  })

  it('rejects INSERT on an existing item (INSERT is not upsert)', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-dup-insert' } })

    // First insert succeeds
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'partiql-dup-insert', 'data': 'first'}`,
    }))

    // Verify item was created
    const check = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-dup-insert' } },
      ConsistentRead: true,
    }))
    expect(check.Item).toBeDefined()
    expect(check.Item!.data.S).toBe('first')

    // Second INSERT with same key should fail with DuplicateItemException
    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'partiql-dup-insert', 'data': 'second'}`,
      })),
      'DuplicateItemException',
    )
  })

  it('INSERT succeeds after DELETE of same key', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-reinsert' } })

    // Insert an item
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'partiql-reinsert', 'data': 'original'}`,
    }))

    // Delete it
    await ddb.send(new ExecuteStatementCommand({
      Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = 'partiql-reinsert'`,
    }))

    // Verify it's gone
    const deleted = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-reinsert' } },
      ConsistentRead: true,
    }))
    expect(deleted.Item).toBeUndefined()

    // INSERT again with same key — should succeed
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': 'partiql-reinsert', 'data': 'reinserted'}`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-reinsert' } },
      ConsistentRead: true,
    }))
    expect(result.Item).toBeDefined()
    expect(result.Item!.data.S).toBe('reinserted')
  })

  it('SELECT returns empty results for non-matching WHERE', async () => {
    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-nonexistent-key'`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(0)
  })

  it('parameterized INSERT with ? placeholders', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-param-insert-1' } })

    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE {'pk': ?, 'data': ?}`,
      Parameters: [
        { S: 'partiql-param-insert-1' },
        { S: 'param-inserted' },
      ],
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-param-insert-1' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeDefined()
    expect(result.Item!.data.S).toBe('param-inserted')
  })

  it('parameterized SELECT with ? placeholder', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-param-select-1' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-param-select-1' }, data: { S: 'found' } },
    }))

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = ?`,
      Parameters: [{ S: 'partiql-param-select-1' }],
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].data.S).toBe('found')
  })

  // ── N1: PartiQL nested path SELECT ──────────────────────────────────

  it('SELECT with nested map path', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-nested' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: {
        pk: { S: 'partiql-nested' },
        mymap: { M: { nested: { S: 'deep' } } },
      },
    }))

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT mymap.nested FROM "${hashTableDef.name}" WHERE pk = 'partiql-nested'`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].nested.S).toBe('deep')
  })

  it('SELECT with specific attributes', async () => {
    // Re-uses 'partiql-nested' item from prior test
    keysToCleanup.push({ pk: { S: 'partiql-nested' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: {
        pk: { S: 'partiql-nested' },
        mymap: { M: { nested: { S: 'deep' } } },
        extra: { S: 'should-not-appear' },
      },
    }))

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT pk, mymap FROM "${hashTableDef.name}" WHERE pk = 'partiql-nested'`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    const item = result.Items![0]
    expect(item.pk.S).toBe('partiql-nested')
    expect(item.mymap).toBeDefined()
    expect(item.extra).toBeUndefined()
  })

  // ── N2: PartiQL begins_with in WHERE ──────────────────────────────────

  it('SELECT with begins_with in WHERE clause', async () => {
    compositeKeysToCleanup.push(
      { pk: { S: 'partiql-bw-test' }, sk: { S: 'prefix-alpha' } },
      { pk: { S: 'partiql-bw-test' }, sk: { S: 'prefix-beta' } },
      { pk: { S: 'partiql-bw-test' }, sk: { S: 'other-gamma' } },
    )

    await Promise.all([
      ddb.send(new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: 'partiql-bw-test' }, sk: { S: 'prefix-alpha' }, data: { S: 'a' } },
      })),
      ddb.send(new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: 'partiql-bw-test' }, sk: { S: 'prefix-beta' }, data: { S: 'b' } },
      })),
      ddb.send(new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: 'partiql-bw-test' }, sk: { S: 'other-gamma' }, data: { S: 'c' } },
      })),
    ])

    const result = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${compositeTableDef.name}" WHERE pk = 'partiql-bw-test' AND begins_with("sk", 'prefix-')`,
    }))

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['prefix-alpha', 'prefix-beta'])
  })

  // ── N3: PartiQL UPDATE with set operations ────────────────────────────

  it('PartiQL UPDATE with SET on attribute', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-update-set' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-update-set' }, myattr: { S: 'oldval' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `UPDATE "${hashTableDef.name}" SET myattr = 'newval' WHERE pk = 'partiql-update-set'`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-update-set' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeDefined()
    expect(result.Item!.myattr.S).toBe('newval')
  })

  it('PartiQL UPDATE with REMOVE', async () => {
    keysToCleanup.push({ pk: { S: 'partiql-update-remove' } })

    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'partiql-update-remove' }, myattr: { S: 'to-remove' }, keep: { S: 'stay' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `UPDATE "${hashTableDef.name}" REMOVE myattr WHERE pk = 'partiql-update-remove'`,
    }))

    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'partiql-update-remove' } },
      ConsistentRead: true,
    }))

    expect(result.Item).toBeDefined()
    expect(result.Item!.myattr).toBeUndefined()
    expect(result.Item!.keep.S).toBe('stay')
  })

  it('returns a populated ConsumedCapacity block when requested', async () => {
    const pk = 'partiql-cc'
    keysToCleanup.push({ pk: { S: pk } })

    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE { 'pk': '${pk}', 'v': 1 }`,
    }))

    const res = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = '${pk}'`,
      ReturnConsumedCapacity: 'TOTAL',
    }))

    // PartiQL always returns the ConsumedCapacity block when asked; some
    // emulators omit it entirely.
    expect(res.ConsumedCapacity).toBeDefined()
    expect(res.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('evaluates negated predicates (NOT begins_with, IS NOT MISSING)', async () => {
    keysToCleanup.push({ pk: { S: 'pq-neg-a' } }, { pk: { S: 'pq-neg-b' } })
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE { 'pk': 'pq-neg-a', 'kind': 'alpha' }`,
    }))
    await ddb.send(new ExecuteStatementCommand({
      Statement: `INSERT INTO "${hashTableDef.name}" VALUE { 'pk': 'pq-neg-b', 'kind': 'beta' }`,
    }))

    // NOT begins_with must evaluate, not fail: only 'beta' does not begin with 'al'.
    const notBw = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk IN ['pq-neg-a','pq-neg-b'] AND NOT begins_with("kind", 'al')`,
    }))
    expect(notBw.Items).toHaveLength(1)
    expect(notBw.Items![0].pk.S).toBe('pq-neg-b')

    // IS NOT MISSING must evaluate: 'kind' is present, so the row matches.
    const notMissing = await ddb.send(new ExecuteStatementCommand({
      Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'pq-neg-a' AND "kind" IS NOT MISSING`,
    }))
    expect(notMissing.Items).toHaveLength(1)
    expect(notMissing.Items![0].pk.S).toBe('pq-neg-a')
  })

  // ── Non-key predicates in PartiQL write WHERE clauses ─────────────────
  // AWS treats a non-key predicate in a DELETE/UPDATE WHERE as a condition that
  // must hold. When the item exists but the predicate is false, the write fails
  // with ConditionalCheckFailedException and the item is left untouched. A
  // missing key is a silent no-op (not ConditionalCheckFailed). The full primary
  // key is mandatory; omitting it is a ValidationException.

  it('DELETE with a false non-key predicate fails ConditionalCheckFailed and leaves the item', async () => {
    const pk = 'pq-pred-del-false'
    keysToCleanup.push({ pk: { S: pk } })
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' }, n: { N: '5' } },
    }))

    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = '${pk}' AND "name" = 'beta'`,
      })),
      'ConditionalCheckFailedException',
    )

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item).toBeDefined()
    expect(after.Item!.name.S).toBe('alpha')
  })

  it('DELETE with a true non-key predicate removes the item', async () => {
    const pk = 'pq-pred-del-true'
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = '${pk}' AND "name" = 'alpha'`,
    }))

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item).toBeUndefined()
  })

  it('UPDATE with a false non-key predicate fails ConditionalCheckFailed and leaves the item', async () => {
    const pk = 'pq-pred-upd-false'
    keysToCleanup.push({ pk: { S: pk } })
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' }, n: { N: '5' } },
    }))

    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `UPDATE "${hashTableDef.name}" SET n = 9 WHERE pk = '${pk}' AND "name" = 'beta'`,
      })),
      'ConditionalCheckFailedException',
    )

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item!.n.N).toBe('5')
  })

  it('UPDATE with a true non-key predicate mutates the item', async () => {
    const pk = 'pq-pred-upd-true'
    keysToCleanup.push({ pk: { S: pk } })
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' }, n: { N: '5' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `UPDATE "${hashTableDef.name}" SET n = 9 WHERE pk = '${pk}' AND "name" = 'alpha'`,
    }))

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item!.n.N).toBe('9')
  })

  it('DELETE with a false NOT begins_with predicate fails ConditionalCheckFailed', async () => {
    const pk = 'pq-pred-fn-false'
    keysToCleanup.push({ pk: { S: pk } })
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' } },
    }))

    // name is 'alpha' which begins with 'al', so NOT begins_with(name, 'al') is false.
    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = '${pk}' AND NOT begins_with("name", 'al')`,
      })),
      'ConditionalCheckFailedException',
    )

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item).toBeDefined()
  })

  it('DELETE with a true NOT begins_with predicate removes the item', async () => {
    const pk = 'pq-pred-fn-true'
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' } },
    }))

    await ddb.send(new ExecuteStatementCommand({
      Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = '${pk}' AND NOT begins_with("name", 'zz')`,
    }))

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item).toBeUndefined()
  })

  it('rejects a write WHERE clause that omits the primary key', async () => {
    const pk = 'pq-pred-nopk'
    keysToCleanup.push({ pk: { S: pk } })
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: pk }, name: { S: 'alpha' } },
    }))

    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE "name" = 'alpha'`,
      })),
      'ValidationException',
      'Where clause does not contain a mandatory equality on all key attributes',
    )
  })

  it('DELETE on a missing key with a non-key predicate is a silent no-op', async () => {
    const pk = 'pq-pred-missing'
    // Ensure the key does not exist.
    await ddb.send(new DeleteItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } },
    }))

    // The item does not exist, so the condition is never evaluated. This succeeds
    // as a no-op rather than raising ConditionalCheckFailedException.
    await ddb.send(new ExecuteStatementCommand({
      Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = '${pk}' AND "name" = 'x'`,
    }))

    const after = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name, Key: { pk: { S: pk } }, ConsistentRead: true,
    }))
    expect(after.Item).toBeUndefined()
  })

  // ── Error tests ───────────────────────────────────────────────────────

  it('rejects a statement with syntax error', async () => {
    await expectDynamoError(
      () => ddb.send(new ExecuteStatementCommand({
        Statement: `SELECTT * FROMM "${hashTableDef.name}"`,
      })),
      'ValidationException',
    )
  })

  it('rejects a reference to a non-existent table', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT * FROM "_conformance_nonexistent_table" WHERE pk = 'x'`,
      }))
      expect.unreachable('should have thrown')
    } catch (e: unknown) {
      expect(e).toBeDefined()
      expect((e as Error).name).toBe('ResourceNotFoundException')
    }
  })
})
