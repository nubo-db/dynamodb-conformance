// Multi-region capture of DynamoDB's negative-input behaviour.
//
// Fires the validation/error inputs the Tier 3 error-message and
// validation-ordering tests care about against real AWS in one or more regions,
// and records the raw err.name / err.message, the "N validation error detected"
// count, the named field list, and the { NULL: false } round-trip. Accepted
// requests record the response body (minus $metadata, binary values normalised
// to { b64, byteLength }), so acceptances carry evidence of the returned shape,
// not just the absence of a throw. Output is a combined JSON document on
// stdout, one block per region.
//
//   AWS_PROFILE=conformance-test node scripts/capture-validation-messages.mjs > capture.json
//   AWS_PROFILE=conformance-test node scripts/capture-validation-messages.mjs eu-west-2 us-east-1 > capture.json
//
// Default regions are the four used for the June 2026 capture. The script
// creates two temporary tables under the _conformance_ prefix per region and
// deletes them by exact name afterwards.
//
// Why it exists: AWS varies validation wording by region and over time. When a
// laggard region is due to flip, re-run this to see what actually changed, and
// draw the contract/cosmetic line for Tier 3 assertions from what is invariant
// across regions rather than from a single region. See docs in CONTRIBUTING
// ("error-messages") and the committed June snapshot in captures/.

import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  BatchWriteItemCommand,
  TransactWriteItemsCommand,
  QueryCommand,
  ScanCommand,
  BatchGetItemCommand,
} from '@aws-sdk/client-dynamodb'

const DEFAULT_REGIONS = ['eu-west-2', 'eu-central-1', 'us-east-1', 'ap-southeast-2']
const regions = process.argv.slice(2).length ? process.argv.slice(2) : DEFAULT_REGIONS
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/** A value nested `depth` maps deep. Mirrors deepMap in the nesting-depth test. */
function deepMap(depth) {
  let v = { S: 'leaf' }
  for (let i = 0; i < depth; i++) v = { M: { n: v } }
  return v
}

function parse(message) {
  if (typeof message !== 'string') return { n: null, fields: [] }
  const m = message.match(/^(\d+) validation error(?:s)? detected:/)
  return { n: m ? Number(m[1]) : null, fields: [...message.matchAll(/at '([^']+)'/g)].map((x) => x[1]) }
}

async function waitActive(ddb, name) {
  const start = Date.now()
  for (;;) {
    const res = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsisActive =
      !res.Table?.GlobalSecondaryIndexes ||
      res.Table.GlobalSecondaryIndexes.every((i) => i.IndexStatus === 'ACTIVE')
    if (res.Table?.TableStatus === 'ACTIVE' && gsisActive) return
    if (Date.now() - start > 120_000) throw new Error(`timeout waiting ACTIVE: ${name}`)
    await sleep(1000)
  }
}

// Everything except $metadata. An acceptance without its response body is only
// half an observation: assertions about the returned shape need the payload.
function stripMetadata(res) {
  if (res == null || typeof res !== 'object') return null
  const { $metadata, ...rest } = res
  return rest
}

// The SDK hands binary (B/BS) values back as Uint8Array, which JSON.stringify
// renders as {} for a zero-length value and {"0":1} for a one-byte one - so a
// raw recording could not distinguish a surviving zero-length member from a
// dropped one. Normalise every Uint8Array in a recorded response to
// { b64, byteLength } so the capture keeps the observation intact. Identity
// for everything else, so responses that carry no binary are unchanged.
function normalizeBinary(v) {
  if (v instanceof Uint8Array) {
    return { b64: Buffer.from(v).toString('base64'), byteLength: v.byteLength }
  }
  if (Array.isArray(v)) return v.map(normalizeBinary)
  if (v !== null && typeof v === 'object') {
    const out = {}
    for (const [k, val] of Object.entries(v)) out[k] = normalizeBinary(val)
    return out
  }
  return v
}

async function probe(id, family, note, fn) {
  try {
    const res = await fn()
    return { id, family, note, threw: false, name: null, message: null, n: null, fields: [], response: normalizeBinary(stripMetadata(res)) }
  } catch (e) {
    const message = e?.message ?? null
    // The reason Message is what the Tier 3 transact tests pin; the top-level
    // message is only the static cancellation wrapper.
    const cancellationReasons = Array.isArray(e?.CancellationReasons)
      ? e.CancellationReasons.map((r) => ({ Code: r?.Code ?? null, Message: r?.Message ?? null }))
      : null
    return { id, family, note, threw: true, name: e?.name ?? null, message, ...parse(message), cancellationReasons }
  }
}

async function captureRegion(region) {
  const ddb = new DynamoDBClient({ region })
  const suffix = `${Date.now()}${Math.floor(Math.random() * 1e6)}`
  const H = `_conformance_capdrift_h_${suffix}`
  const C = `_conformance_capdrift_c_${suffix}`
  const CT3 = `_conformance_capdrift_ct3_${suffix}`
  const pt = { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }

  // H carries a KEYS_ONLY GSI so the projection family can probe reads that ask
  // an index for an attribute the index does not project.
  await ddb.send(new CreateTableCommand({
    TableName: H,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'gpk', AttributeType: 'S' },
    ],
    KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
    GlobalSecondaryIndexes: [
      {
        IndexName: 'gidx',
        KeySchema: [{ AttributeName: 'gpk', KeyType: 'HASH' }],
        Projection: { ProjectionType: 'KEYS_ONLY' },
      },
    ],
  }))
  await ddb.send(new CreateTableCommand({ TableName: C, BillingMode: 'PAY_PER_REQUEST', AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }, { AttributeName: 'sk', AttributeType: 'S' }], KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }, { AttributeName: 'sk', KeyType: 'RANGE' }] }))
  await waitActive(ddb, H)
  await waitActive(ddb, C)

  // Canonical seed for the projection family: sibling scalars and a nested map
  // under one top-level attribute, plus a list of maps whose elements carry
  // sibling scalars. Accepted projections resolve against this item, so their
  // recorded response bodies show the returned shape, not an empty result.
  const PROJ_PK = 'proj-val'
  const PROJ_GPK = 'proj-val-g'
  await ddb.send(new PutItemCommand({
    TableName: H,
    Item: {
      pk: { S: PROJ_PK },
      gpk: { S: PROJ_GPK },
      a: { M: { b: { S: 'bb' }, c: { S: 'cc' }, d: { M: { e: { S: 'ee' } } } } },
      l: {
        L: [
          { M: { x: { S: 'x0' }, y: { S: 'y0' } } },
          { M: { x: { S: 'x1' }, y: { S: 'y1' } } },
        ],
      },
    },
  }))

  const probes = []
  const p = (id, family, note, fn) => probe(id, family, note, fn).then((r) => probes.push(r))
  try {
    // scalar echo (tableName)
    await p('s_put_table_null', 'echo-scalar', 'PutItem TableName=undefined', () => ddb.send(new PutItemCommand({ TableName: undefined, Item: { pk: { S: 'test' } } })))
    await p('s_put_table_empty', 'echo-scalar', "PutItem TableName=''", () => ddb.send(new PutItemCommand({ TableName: '', Item: { pk: { S: 'test' } } })))
    await p('s_put_table_long', 'echo-scalar', "PutItem TableName='a'*256", () => ddb.send(new PutItemCommand({ TableName: 'a'.repeat(256), Item: { pk: { S: 'test' } } })))
    await p('s_put_table_badchars', 'echo-scalar', "PutItem TableName='bad table!@#'", () => ddb.send(new PutItemCommand({ TableName: 'bad table!@#', Item: { pk: { S: 'test' } } })))
    await p('s_ct_table_short', 'echo-scalar', "CreateTable TableName='ab'", () => ddb.send(new CreateTableCommand({ TableName: 'ab', AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }], KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }], ProvisionedThroughput: pt })))
    // collection echo
    await p('c_bw_over25', 'echo-collection', 'BatchWrite 26 requests', () => ddb.send(new BatchWriteItemCommand({ RequestItems: { [H]: Array.from({ length: 26 }, (_, i) => ({ PutRequest: { Item: { pk: { S: `bw-${i}` } } } })) } })))
    await p('c_ct_3keys', 'echo-collection', 'CreateTable 3 keySchema', () => ddb.send(new CreateTableCommand({ TableName: CT3, AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }, { AttributeName: 'sk', AttributeType: 'S' }, { AttributeName: 'extra', AttributeType: 'S' }], KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }, { AttributeName: 'sk', KeyType: 'RANGE' }, { AttributeName: 'extra', KeyType: 'RANGE' }], ProvisionedThroughput: pt })))
    // bespoke putItem
    await p('b_put_empty_ss', 'bespoke', 'empty string set', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'test' }, bad: { SS: [] } } })))
    await p('b_put_empty_ns', 'bespoke', 'empty number set', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'test' }, bad: { NS: [] } } })))
    await p('b_put_dup_ss', 'bespoke', 'duplicate SS', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'test' }, bad: { SS: ['a', 'a'] } } })))
    await p('b_put_mix_expr', 'bespoke', 'mixing expression and non-expression', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'test' } }, Expected: { pk: { Exists: false } }, ConditionExpression: 'attribute_not_exists(pk)' })))
    await p('b_put_eav_no_expr', 'bespoke', 'EAV without expression', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'test' } }, ExpressionAttributeValues: { ':v': { S: 'unused' } } })))
    await p('b_put_redundant_parens', 'bespoke', 'redundant parentheses', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'em-put-redundant' } }, ConditionExpression: '((attribute_not_exists(pk)))' })))
    await p('b_put_contains_dup', 'bespoke', 'contains distinct operand', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'em-put-contains-dup' } }, ConditionExpression: 'contains(#a, #a)', ExpressionAttributeNames: { '#a': 'data' } })))
    // bespoke updateItem
    await p('u_upd_hashkey', 'bespoke', 'cannot update hash key', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'SET pk = :v', ExpressionAttributeValues: { ':v': { S: 'new-val' } } })))
    await p('u_upd_syntax', 'bespoke', 'invalid UpdateExpression syntax', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'INVALID SYNTAX HERE', ExpressionAttributeValues: { ':v': { S: 'val' } } })))
    await p('u_upd_unused_ean', 'bespoke', 'unused EAN', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'val' } }, ExpressionAttributeNames: { '#unused': 'someattr' } })))
    await p('u_upd_unused_eav', 'bespoke', 'unused EAV', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'val' }, ':unused': { S: 'extra' } } })))
    await p('u_upd_missing_eav', 'bespoke', 'missing EAV ref', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'SET attr1 = :v' })))
    await p('u_upd_mix', 'bespoke', 'mixing UpdateExpression + AttributeUpdates', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'val' } }, AttributeUpdates: { attr1: { Value: { S: 'val' }, Action: 'PUT' } } })))
    await p('u_upd_empty', 'bespoke', 'empty UpdateExpression', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'em-upd-key-mod' } }, UpdateExpression: '' })))
    await p('u_upd_rangekey', 'bespoke', 'cannot update range key', () => ddb.send(new UpdateItemCommand({ TableName: C, Key: { pk: { S: 'em-upd-range-mod' }, sk: { S: 'sk1' } }, UpdateExpression: 'SET sk = :v', ExpressionAttributeValues: { ':v': { S: 'new-sk' } } })))
    // validation-ordering / N count
    await p('o_put_empty_table_empty_item', 'ordering', "PutItem TableName='' Item={}", () => ddb.send(new PutItemCommand({ TableName: '', Item: {} })))
    await p('o_put_empty_table_bad_rv', 'ordering', "PutItem TableName='' ReturnValues=INVALID", () => ddb.send(new PutItemCommand({ TableName: '', ReturnValues: 'INVALID', Item: {} })))
    await p('o_put_three_bad_enums', 'ordering', 'PutItem 3 invalid enums', () => ddb.send(new PutItemCommand({ TableName: '_conformance_valid_table_name', Item: { pk: { S: 'test' } }, ReturnConsumedCapacity: 'INVALID', ReturnItemCollectionMetrics: 'INVALID', ReturnValues: 'INVALID' })))
    await p('o_del_empty_table', 'ordering', "DeleteItem TableName='' Key={}", () => ddb.send(new DeleteItemCommand({ TableName: '', Key: {} })))
    await p('o_del_two_bad_enums', 'ordering', 'DeleteItem 2 invalid enums', () => ddb.send(new DeleteItemCommand({ TableName: '_conformance_valid_table_name', Key: { pk: { S: 'test' } }, ReturnValues: 'INVALID', ReturnConsumedCapacity: 'INVALID' })))
    await p('o_upd_empty_table', 'ordering', "UpdateItem TableName='' Key={}", () => ddb.send(new UpdateItemCommand({ TableName: '', Key: {} })))
    await p('o_upd_two_bad_enums', 'ordering', 'UpdateItem 2 invalid enums', () => ddb.send(new UpdateItemCommand({ TableName: '_conformance_valid_table_name', Key: { pk: { S: 'test' } }, ReturnValues: 'INVALID', ReturnConsumedCapacity: 'INVALID' })))
    // Two behaviours the weekly sweep raised as split candidates and could not
    // then admit, because a row records what a region answered and nothing
    // captured that. Both mirror their test's request exactly, so the answer
    // recorded here is the one the committed assertion compares against.
    await p('o_bg_empty_requestitems', 'ordering', 'BatchGetItem RequestItems={}', () => ddb.send(new BatchGetItemCommand({ RequestItems: {} })))
    // No seed: a region that rejects the 32-level value answers with the
    // nesting ValidationException whether the item is there or not, and one
    // that accepts it evaluates the condition and answers
    // ConditionalCheckFailedException either way. The two answers are what
    // separate the cohorts, and both survive an absent item.
    await p('o_upd_nested_32_eav', 'ordering', 'UpdateItem 32-level ExpressionAttributeValue', () => ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: 'capdrift-nest-cond' } }, UpdateExpression: 'SET touched = :t', ConditionExpression: '#d = :deep', ExpressionAttributeNames: { '#d': 'data' }, ExpressionAttributeValues: { ':t': { S: 'y' }, ':deep': deepMap(32) } })))

    // Invalid key-VALUE coverage for the batch / lookup / transact paths.
    // batch-key: real AWS collapses wrong-type and non-scalar table keys to the
    // generic schema-mismatch message, not PutItem's 'Type mismatch for key' form.
    await p('bw_table_wrongtype', 'batch-key', 'BatchWrite PutRequest pk wrong type (N on S)', () => ddb.send(new BatchWriteItemCommand({ RequestItems: { [H]: [{ PutRequest: { Item: { pk: { N: '5' } } } }] } })))
    await p('bw_table_nonscalar', 'batch-key', 'BatchWrite PutRequest pk non-scalar (L)', () => ddb.send(new BatchWriteItemCommand({ RequestItems: { [H]: [{ PutRequest: { Item: { pk: { L: [{ S: 'x' }] } } } }] } })))

    const badKeys = [
      ['empty', { S: '' }],
      ['wrongtype', { N: '5' }],
      ['nonscalar', { L: [{ S: 'x' }] }],
    ]

    // lookup-key baseline: the non-transactional behaviour the transact lookup-key
    // cases mirror, captured alongside them. A read may return no item, not throw.
    for (const [k, val] of badKeys) {
      await p(`del_key_${k}`, 'lookup-key', `DeleteItem Key pk ${k}`, () => ddb.send(new DeleteItemCommand({ TableName: H, Key: { pk: val } })))
      await p(`get_key_${k}`, 'lookup-key', `GetItem Key pk ${k}`, () => ddb.send(new GetItemCommand({ TableName: H, Key: { pk: val } })))
    }

    // transact-key: the key-only validation path. ConditionCheck carries an extra
    // condition stage, so its rows are verified independently of Update / Delete.
    for (const [k, val] of badKeys) {
      await p(`twi_upd_key_${k}`, 'transact-key', `TransactWrite Update Key pk ${k}`, () => ddb.send(new TransactWriteItemsCommand({ TransactItems: [{ Update: { TableName: H, Key: { pk: val }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'x' } } } }] })))
      await p(`twi_del_key_${k}`, 'transact-key', `TransactWrite Delete Key pk ${k}`, () => ddb.send(new TransactWriteItemsCommand({ TransactItems: [{ Delete: { TableName: H, Key: { pk: val } } }] })))
      await p(`twi_cc_key_${k}`, 'transact-key', `TransactWrite ConditionCheck Key pk ${k}`, () => ddb.send(new TransactWriteItemsCommand({ TransactItems: [{ ConditionCheck: { TableName: H, Key: { pk: val }, ConditionExpression: 'attribute_not_exists(pk)' } }] })))
    }

    // ProjectionExpression validation matrix, fired identically at GetItem,
    // Query, Scan and BatchGetItem. Duplicate paths, overlapping parent/child
    // paths, alias collisions, and the shared-prefix shapes that are legal.
    // Query and Scan run against the seeded item (matching partition/filter),
    // so an acceptance records the projected shape; the projection-eager cells
    // below re-fire selected cases against zero-match requests. No outcome is
    // assumed: an acceptance and a rejection are both findings.
    const projCases = [
      ['d1', "raw duplicate 'a, a'", 'a, a', null],
      ['d2', "same alias twice '#a, #a'", '#a, #a', { '#a': 'a' }],
      ['d3', "distinct aliases, one attribute '#a, #b' (both -> a)", '#a, #b', { '#a': 'a', '#b': 'a' }],
      ['d4', "raw plus alias 'a, #a'", 'a, #a', { '#a': 'a' }],
      ['d5', "aliased nested duplicate '#a.#b, #x.#y' (both -> a.b)", '#a.#b, #x.#y', { '#a': 'a', '#b': 'b', '#x': 'a', '#y': 'b' }],
      ['o1', "raw parent/child 'a, a.b'", 'a, a.b', null],
      ['o2', "aliased parent/child '#a, #a.#b'", '#a, #a.#b', { '#a': 'a', '#b': 'b' }],
      ['o3', "child before parent 'a.b, a'", 'a.b, a', null],
      ['o4', "cross-alias overlap '#x, #y.#b' (#x, #y -> a)", '#x, #y.#b', { '#x': 'a', '#y': 'a', '#b': 'b' }],
      ['o5', "list parent and index 'l, l[0]'", 'l, l[0]', null],
      ['o6', "deep overlap 'a, a.b.c'", 'a, a.b.c', null],
      ['a1', "sibling map paths 'a.b, a.c'", 'a.b, a.c', null],
      ['a2', "same-index list siblings 'l[0].x, l[0].y'", 'l[0].x, l[0].y', null],
      ['a3', "distinct list indices 'l[0], l[1]'", 'l[0], l[1]', null],
      ['a4', "unrelated top-level attrs 'a, l'", 'a, l', null],
      ['undef', "undefined alias '#undef'", '#undef', null],
    ]
    for (const [cid, note, expr, names] of projCases) {
      const ean = names ? { ExpressionAttributeNames: names } : {}
      await p(`proj_get_${cid}`, 'projection', `GetItem ${note}`, () => ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: PROJ_PK } }, ConsistentRead: true, ProjectionExpression: expr, ...ean })))
      await p(`proj_query_${cid}`, 'projection', `Query (matching partition) ${note}`, () => ddb.send(new QueryCommand({ TableName: H, KeyConditionExpression: 'pk = :pk', ExpressionAttributeValues: { ':pk': { S: PROJ_PK } }, ConsistentRead: true, ProjectionExpression: expr, ...ean })))
      await p(`proj_scan_${cid}`, 'projection', `Scan (matching filter) ${note}`, () => ddb.send(new ScanCommand({ TableName: H, FilterExpression: 'pk = :scope', ExpressionAttributeValues: { ':scope': { S: PROJ_PK } }, ConsistentRead: true, ProjectionExpression: expr, ...ean })))
      await p(`proj_bg_${cid}`, 'projection', `BatchGetItem ${note}`, () => ddb.send(new BatchGetItemCommand({ RequestItems: { [H]: { Keys: [{ pk: { S: PROJ_PK } }], ConsistentRead: true, ProjectionExpression: expr, ...ean } } })))
    }

    // projection-eager: the same rejection classes against requests that match
    // nothing. A validator that only checks the projection per emitted row
    // returns an empty result here instead of throwing; the matching-row cells
    // above are the controls proving any rejection is not an artefact of the
    // empty result.
    await p('proj_query_zero_d1', 'projection-eager', "Query matching no partition, raw duplicate 'a, a'", () => ddb.send(new QueryCommand({ TableName: H, KeyConditionExpression: 'pk = :pk', ExpressionAttributeValues: { ':pk': { S: 'no-such-partition-proj' } }, ConsistentRead: true, ProjectionExpression: 'a, a' })))
    await p('proj_query_zero_o1', 'projection-eager', "Query matching no partition, overlap 'a, a.b'", () => ddb.send(new QueryCommand({ TableName: H, KeyConditionExpression: 'pk = :pk', ExpressionAttributeValues: { ':pk': { S: 'no-such-partition-proj' } }, ConsistentRead: true, ProjectionExpression: 'a, a.b' })))
    await p('proj_scan_zero_d1', 'projection-eager', "Scan filter matching nothing, raw duplicate 'a, a'", () => ddb.send(new ScanCommand({ TableName: H, FilterExpression: 'pk = :never', ExpressionAttributeValues: { ':never': { S: 'no-such-pk-proj' } }, ConsistentRead: true, ProjectionExpression: 'a, a' })))
    await p('proj_scan_zero_o1', 'projection-eager', "Scan filter matching nothing, overlap 'a, a.b'", () => ddb.send(new ScanCommand({ TableName: H, FilterExpression: 'pk = :never', ExpressionAttributeValues: { ':never': { S: 'no-such-pk-proj' } }, ConsistentRead: true, ProjectionExpression: 'a, a.b' })))
    await p('proj_bg_nomatch_d1', 'projection-eager', "BatchGetItem key matching no item, raw duplicate 'a, a'", () => ddb.send(new BatchGetItemCommand({ RequestItems: { [H]: { Keys: [{ pk: { S: 'no-such-item-proj' } }], ConsistentRead: true, ProjectionExpression: 'a, a' } } })))

    // Cross-entry: a bad projection on one table entry alongside a clean one on
    // another, probing whether the whole batch rejects.
    await p('proj_bg_b1_crossentry', 'projection', "BatchGetItem bad projection ('a, a') on one entry, clean ('a') on another", () => ddb.send(new BatchGetItemCommand({ RequestItems: {
      [H]: { Keys: [{ pk: { S: PROJ_PK } }], ConsistentRead: true, ProjectionExpression: 'a, a' },
      [C]: { Keys: [{ pk: { S: 'proj-val-c' }, sk: { S: 's' } }], ConsistentRead: true, ProjectionExpression: 'a' },
    } })))

    // projection-gsi: a read asking a KEYS_ONLY index for an attribute it does
    // not project. Wait (bounded, best-effort) for the seeded item to appear in
    // the eventually-consistent index so an acceptance reflects index content
    // rather than an empty read.
    const gsiDeadline = Date.now() + 30_000
    for (;;) {
      const res = await ddb.send(new QueryCommand({ TableName: H, IndexName: 'gidx', KeyConditionExpression: 'gpk = :g', ExpressionAttributeValues: { ':g': { S: PROJ_GPK } } })).catch(() => null)
      if ((res?.Count ?? 0) >= 1 || Date.now() > gsiDeadline) break
      await sleep(1000)
    }
    await p('proj_gsi_query_nonprojected', 'projection-gsi', "Query on KEYS_ONLY GSI projecting non-projected attribute 'a'", () => ddb.send(new QueryCommand({ TableName: H, IndexName: 'gidx', KeyConditionExpression: 'gpk = :g', ExpressionAttributeValues: { ':g': { S: PROJ_GPK } }, ProjectionExpression: 'a' })))
    await p('proj_gsi_scan_nonprojected', 'projection-gsi', "Scan on KEYS_ONLY GSI projecting non-projected attribute 'a'", () => ddb.send(new ScanCommand({ TableName: H, IndexName: 'gidx', ProjectionExpression: 'a' })))

    // Empty members inside a non-empty set (SS/BS/NS). The AWS docs state both
    // halves in one sentence - "DynamoDB does not support empty sets, however,
    // empty string and binary values are allowed within a set" - and until this
    // family only the first half had probes. Acceptance probes seed their own
    // state inside the probe function (so a rejection anywhere in the chain is
    // recorded rather than crashing the region sweep), write under their own
    // esm-* partition key so no two probes clobber each other, and resolve to a
    // ConsistentRead GetItem on that key: the record shows not just "accepted"
    // but exactly what came back. Rejection probes record name/message as usual.
    const EMPTY_BIN = new Uint8Array(0)
    const esmPutThenGet = async (pk, attrs) => {
      await ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: pk }, ...attrs } }))
      const got = await ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: pk } }, ConsistentRead: true }))
      return { Item: got.Item ?? null }
    }
    const esmUpdThenGet = async (pk, seedAttrs, update) => {
      await ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: pk }, ...seedAttrs } }))
      await ddb.send(new UpdateItemCommand({ TableName: H, Key: { pk: { S: pk } }, ...update }))
      const got = await ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: pk } }, ConsistentRead: true }))
      return { Item: got.Item ?? null }
    }

    // PutItem acceptance cells: sole empty member, mixed, binary, and nested.
    await p('esm_put_ss_only', 'empty-set-member', "PutItem SS [''] (sole member empty)", () => esmPutThenGet('esm-put-ss-only', { attr: { SS: [''] } }))
    await p('esm_put_ss_mixed', 'empty-set-member', "PutItem SS ['', 'a']", () => esmPutThenGet('esm-put-ss-mixed', { attr: { SS: ['', 'a'] } }))
    await p('esm_put_bs_only', 'empty-set-member', 'PutItem BS [zero-length]', () => esmPutThenGet('esm-put-bs-only', { attr: { BS: [EMPTY_BIN] } }))
    await p('esm_put_bs_mixed', 'empty-set-member', 'PutItem BS [zero-length, 0x01]', () => esmPutThenGet('esm-put-bs-mixed', { attr: { BS: [EMPTY_BIN, new Uint8Array([1])] } }))
    await p('esm_put_map_ss', 'empty-set-member', "PutItem M { inner: SS [''] }", () => esmPutThenGet('esm-put-map-ss', { outer: { M: { inner: { SS: [''] } } } }))
    await p('esm_put_list_ss', 'empty-set-member', "PutItem L [ SS [''] ]", () => esmPutThenGet('esm-put-list-ss', { items: { L: [{ SS: [''] }] } }))

    // UpdateItem cells: SET builds the set, a document-path SET revalidates
    // through the expression engine, ADD/DELETE mutate membership. The
    // delete-to-empty-member cell produces a set whose only member is the empty
    // string as the *output* of a server-side mutation.
    await p('esm_upd_set', 'empty-set-member', "UpdateItem SET tags = SS ['']", () => esmUpdThenGet('esm-upd-set', {}, { UpdateExpression: 'SET tags = :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    // 'outer' and 'inner' are both reserved words, so the document path is aliased.
    await p('esm_upd_set_nested', 'empty-set-member', "UpdateItem SET outer.inner = SS [''] on an existing map", () => esmUpdThenGet('esm-upd-set-nested', { outer: { M: {} } }, { UpdateExpression: 'SET #o.#i = :v', ExpressionAttributeNames: { '#o': 'outer', '#i': 'inner' }, ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_add_existing', 'empty-set-member', "UpdateItem ADD tags SS [''] onto SS ['a']", () => esmUpdThenGet('esm-upd-add-existing', { tags: { SS: ['a'] } }, { UpdateExpression: 'ADD tags :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_add_new', 'empty-set-member', "UpdateItem ADD tags SS [''] onto a missing attribute", () => esmUpdThenGet('esm-upd-add-new', {}, { UpdateExpression: 'ADD tags :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_add_dup', 'empty-set-member', "UpdateItem ADD tags SS [''] onto SS [''] (already present)", () => esmUpdThenGet('esm-upd-add-dup', { tags: { SS: [''] } }, { UpdateExpression: 'ADD tags :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_add_bs_existing', 'empty-set-member', 'UpdateItem ADD bins BS [zero-length] onto BS [0x01]', () => esmUpdThenGet('esm-upd-add-bs-existing', { bins: { BS: [new Uint8Array([1])] } }, { UpdateExpression: 'ADD bins :v', ExpressionAttributeValues: { ':v': { BS: [EMPTY_BIN] } } }))
    await p('esm_upd_del_member', 'empty-set-member', "UpdateItem DELETE tags SS [''] from SS ['', 'a']", () => esmUpdThenGet('esm-upd-del-member', { tags: { SS: ['', 'a'] } }, { UpdateExpression: 'DELETE tags :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_del_last', 'empty-set-member', "UpdateItem DELETE tags SS [''] from SS [''] (last member)", () => esmUpdThenGet('esm-upd-del-last', { tags: { SS: [''] } }, { UpdateExpression: 'DELETE tags :v', ExpressionAttributeValues: { ':v': { SS: [''] } } }))
    await p('esm_upd_del_to_empty_member', 'empty-set-member', "UpdateItem DELETE tags SS ['a'] from SS ['', 'a'] (leaves only the empty member)", () => esmUpdThenGet('esm-upd-del-to-empty-member', { tags: { SS: ['', 'a'] } }, { UpdateExpression: 'DELETE tags :v', ExpressionAttributeValues: { ':v': { SS: ['a'] } } }))

    // Multi-item write paths, which may revalidate the item separately from the
    // single-item PutItem path.
    await p('esm_bw_put', 'empty-set-member', "BatchWriteItem PutRequest SS ['']", async () => {
      const bw = await ddb.send(new BatchWriteItemCommand({ RequestItems: { [H]: [{ PutRequest: { Item: { pk: { S: 'esm-bw-put' }, attr: { SS: [''] } } } }] } }))
      const got = await ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: 'esm-bw-put' } }, ConsistentRead: true }))
      return { UnprocessedItems: bw.UnprocessedItems ?? null, Item: got.Item ?? null }
    })
    await p('esm_twi_put', 'empty-set-member', "TransactWriteItems Put SS ['']", async () => {
      await ddb.send(new TransactWriteItemsCommand({ TransactItems: [{ Put: { TableName: H, Item: { pk: { S: 'esm-twi-put' }, attr: { SS: [''] } } } }] }))
      const got = await ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: 'esm-twi-put' } }, ConsistentRead: true }))
      return { Item: got.Item ?? null }
    })

    // contains(set, '') membership, with the control that gives the hit meaning.
    // Runs as a Query on the composite table scoped to a dedicated pk and the
    // probe's own sk, never a Scan on H - a Scan would sweep the esm_put_* /
    // esm_upd_* items just written above, several of which hold an empty member,
    // so the negative control could never return zero matches.
    const esmContainsQuery = async (sk, tags) => {
      await ddb.send(new PutItemCommand({ TableName: C, Item: { pk: { S: 'esm-contains' }, sk: { S: sk }, tags: { SS: tags } } }))
      const res = await ddb.send(new QueryCommand({
        TableName: C,
        KeyConditionExpression: 'pk = :pk AND sk = :sk',
        FilterExpression: 'contains(tags, :e)',
        ExpressionAttributeValues: { ':pk': { S: 'esm-contains' }, ':sk': { S: sk }, ':e': { S: '' } },
        ConsistentRead: true,
      }))
      return { Count: res.Count ?? null, ScannedCount: res.ScannedCount ?? null, Items: res.Items ?? null }
    }
    await p('esm_query_contains_hit', 'empty-set-member', "Query contains(tags, '') against SS ['', 'a']", () => esmContainsQuery('with-empty', ['', 'a']))
    await p('esm_query_contains_miss', 'empty-set-member', "Query contains(tags, '') against SS ['a'] (negative control)", () => esmContainsQuery('without', ['a']))

    // Rejection cells: an empty string is not a number, duplicate empty members
    // are still duplicates, and the empty-set controls - the message an
    // over-strict target wrongly returns for [''] , including the never-before
    // probed empty binary set.
    await p('esm_put_ns_empty', 'empty-set-member', "PutItem NS ['']", () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'esm-rej-ns-empty' }, attr: { NS: [''] } } })))
    await p('esm_put_ss_dup_empty', 'empty-set-member', "PutItem SS ['', ''] (duplicate empty members)", () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'esm-rej-ss-dup' }, attr: { SS: ['', ''] } } })))
    await p('esm_put_bs_dup_empty', 'empty-set-member', 'PutItem BS [zero-length, zero-length] (duplicate empty members)', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'esm-rej-bs-dup' }, attr: { BS: [EMPTY_BIN, EMPTY_BIN] } } })))
    await p('esm_put_ss_empty_set', 'empty-set-member', 'PutItem SS [] (empty-set control)', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'esm-rej-ss-empty-set' }, attr: { SS: [] } } })))
    await p('esm_put_bs_empty_set', 'empty-set-member', 'PutItem BS [] (empty-set control)', () => ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'esm-rej-bs-empty-set' }, attr: { BS: [] } } })))

    // { NULL: false } round-trip
    let nullRoundTrip
    try {
      await ddb.send(new PutItemCommand({ TableName: H, Item: { pk: { S: 'em-put-null-false' }, attr1: { NULL: false } } }))
      const got = await ddb.send(new GetItemCommand({ TableName: H, Key: { pk: { S: 'em-put-null-false' } }, ConsistentRead: true }))
      nullRoundTrip = { put: 'accepted', returnedItem: got.Item ?? null }
    } catch (e) {
      nullRoundTrip = { put: 'rejected', name: e?.name ?? null, message: e?.message ?? null }
    }
    return { region, probes, nullRoundTrip }
  } finally {
    for (const name of [H, C, CT3]) {
      try {
        await ddb.send(new DeleteTableCommand({ TableName: name }))
      } catch {
        // best-effort; CT3 and any never-created table will ResourceNotFound
      }
    }
  }
}

async function main() {
  const out = { capturedAt: new Date().toISOString(), regions: {} }
  for (const region of regions) {
    process.stderr.write(`capturing ${region}...\n`)
    out.regions[region] = await captureRegion(region)
  }
  process.stdout.write(JSON.stringify(out, null, 2) + '\n')
}

main().catch((e) => {
  console.error('CAPTURE FAILED:', e?.name, e?.message)
  process.exit(1)
})
