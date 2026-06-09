// Multi-region capture of DynamoDB's negative-input behaviour.
//
// Fires the validation/error inputs the Tier 3 error-message and
// validation-ordering tests care about against real AWS in one or more regions,
// and records the raw err.name / err.message, the "N validation error detected"
// count, the named field list, and the { NULL: false } round-trip. Output is a
// combined JSON document on stdout, one block per region.
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
} from '@aws-sdk/client-dynamodb'

const DEFAULT_REGIONS = ['eu-west-2', 'eu-central-1', 'us-east-1', 'ap-southeast-2']
const regions = process.argv.slice(2).length ? process.argv.slice(2) : DEFAULT_REGIONS
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function parse(message) {
  if (typeof message !== 'string') return { n: null, fields: [] }
  const m = message.match(/^(\d+) validation error(?:s)? detected:/)
  return { n: m ? Number(m[1]) : null, fields: [...message.matchAll(/at '([^']+)'/g)].map((x) => x[1]) }
}

async function waitActive(ddb, name) {
  const start = Date.now()
  for (;;) {
    const res = await ddb.send(new DescribeTableCommand({ TableName: name }))
    if (res.Table?.TableStatus === 'ACTIVE') return
    if (Date.now() - start > 120_000) throw new Error(`timeout waiting ACTIVE: ${name}`)
    await sleep(1000)
  }
}

async function probe(id, family, note, fn) {
  try {
    await fn()
    return { id, family, note, threw: false, name: null, message: null, n: null, fields: [] }
  } catch (e) {
    const message = e?.message ?? null
    return { id, family, note, threw: true, name: e?.name ?? null, message, ...parse(message) }
  }
}

async function captureRegion(region) {
  const ddb = new DynamoDBClient({ region })
  const suffix = `${Date.now()}${Math.floor(Math.random() * 1e6)}`
  const H = `_conformance_capdrift_h_${suffix}`
  const C = `_conformance_capdrift_c_${suffix}`
  const CT3 = `_conformance_capdrift_ct3_${suffix}`
  const pt = { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }

  await ddb.send(new CreateTableCommand({ TableName: H, BillingMode: 'PAY_PER_REQUEST', AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }], KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }] }))
  await ddb.send(new CreateTableCommand({ TableName: C, BillingMode: 'PAY_PER_REQUEST', AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }, { AttributeName: 'sk', AttributeType: 'S' }], KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }, { AttributeName: 'sk', KeyType: 'RANGE' }] }))
  await waitActive(ddb, H)
  await waitActive(ddb, C)

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
