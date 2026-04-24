# DynamoDB Conformance Suite

An independent test suite that validates any DynamoDB-compatible endpoint against real DynamoDB behaviour. It works against DynamoDB, DynamoDB Local, Dynoxide, Dynalite, LocalStack, or anything else that implements the DynamoDB HTTP API.

## Why this exists

There's no official AWS conformance suite for DynamoDB. The closest thing the community has is Dynalite's test suite, but over half of its tests are stale against current DynamoDB behaviour (verified March 2026). DynamoDB Local ships with no test suite at all. Every emulator author ends up guessing at behaviour and testing against their own assumptions.

This suite fixes that by running every test against real DynamoDB first, recording what passes, and using those results as the baseline. An emulator only passes if it gives the same answer DynamoDB does.

## Quick start

```bash
npm install

# Run against a local target
DYNAMODB_ENDPOINT=http://localhost:8000 npm test

# Quicker run, excludes GSI lifecycle tests (see runtime notes below)
DYNAMODB_ENDPOINT=http://localhost:8000 npm run test:quick

# Run a specific tier
DYNAMODB_ENDPOINT=http://localhost:8000 npm run test:tier1
```

## Results

| Target | Tier 1 | Tier 2 | Tier 3 | Total | Pass/Fail |
|--------|--------|--------|--------|-------|-----------|
| DynamoDB | 100% | 100% | 100% | 100% | 572/0 (ground truth) |
| Dynoxide | 100% | 100% | 100% | 100% | 572/0 |
| LocalStack | 99.0% | 96.1% | 81.9% | 93.5% | 535/37 |
| DynamoDB Local | 99.0% | 91.3% | 81.9% | 92.7% | 530/42 |
| Dynalite | 98.3% | 9.7% | 92.8% | 80.8% | 462/67 |

Regenerate with `npm run results:table`.

## Tiers

**Tier 1 - Core.** The operations and behaviours that 90% of DynamoDB users rely on. CRUD, queries, scans, batch operations, GSIs, UpdateTable. If an emulator fails Tier 1, it's not usable.

**Tier 2 - Complete.** Less common but documented features. Transactions, PartiQL, LSIs, TTL, streams, tags. An emulator that passes Tier 1 but fails some Tier 2 is usable with caveats.

**Tier 3 - Strict.** Validation ordering, exact error message formatting, edge cases around limits, legacy API compatibility (ScanFilter, QueryFilter). An emulator that passes Tier 1 and Tier 2 but fails some Tier 3 is production-quality for local dev.

The tiers give emulator authors something meaningful to report. "100% Tier 1, 95% Tier 2, 80% Tier 3" tells you far more than a single percentage.

## Running against targets

### DynamoDB Local

```bash
docker run -d --name ddb-local -p 8000:8000 amazon/dynamodb-local:latest
DYNAMODB_ENDPOINT=http://localhost:8000 npm test
docker stop ddb-local && docker rm ddb-local
```

### Dynoxide

```bash
dynoxide --port 8001 &
DYNAMODB_ENDPOINT=http://localhost:8001 npm test
kill %1
```

### Dynalite

```bash
npx dynalite --port 8002 &
DYNAMODB_ENDPOINT=http://localhost:8002 npm test
kill %1
```

### LocalStack

LocalStack requires a free account. Sign up at [localstack.cloud](https://www.localstack.cloud) and set your auth token.

```bash
export LOCALSTACK_AUTH_TOKEN=your-token-here
docker run -d --name localstack -p 4566:4566 -e LOCALSTACK_AUTH_TOKEN localstack/localstack
DYNAMODB_ENDPOINT=http://localhost:4566 npm test
docker stop localstack && docker rm localstack
```

### Real DynamoDB

```bash
# Uses the default AWS credential chain (env vars, ~/.aws/credentials, IAM role)
npm test
```

## Expected runtime

| Target | `npm test` | `npm run test:quick` |
|--------|-----------|---------------------|
| Local emulators | ~2-5 seconds | ~2-5 seconds |
| Real DynamoDB | ~60-90 minutes | ~20-25 minutes |

The full suite includes 11 UpdateTable GSI lifecycle tests that add and remove Global Secondary Indexes from existing tables. On real DynamoDB, each GSI creation triggers a backfill that takes 5-15 minutes even on empty tables. These tests are important for conformance but they dominate runtime against real AWS.

`test:quick` excludes the GSI lifecycle tests for faster iteration. CI uses `test:quick` for the real DynamoDB job to save billable AWS time. Emulator targets run the full `npm test` since GSI creation is instant locally. If you're modifying GSI-related code, run the full suite against real DynamoDB manually before merging.

## Design principles

**Ground truth first.** Every test is validated against real DynamoDB. If DynamoDB's behaviour changes, the suite updates.

**Observable behaviour only.** Tests verify what comes back over the wire: response bodies, error types, error messages. No testing of internal implementation details.

**SDK-driven.** Tests use the AWS SDK v3 for JavaScript rather than raw HTTP. This tests what real applications actually experience.

**Endpoint-agnostic.** A single environment variable (`DYNAMODB_ENDPOINT`) points the suite at any target. No target-specific code paths, no special cases.

## Test organisation

```
tests/
  tier1/                    # ~290 tests
    createTable/            # basic, gsi, lsi
    putItem/                # basic, conditions, validation, expressions, dataTypes, ...
    getItem/                # basic, validation, projection, consumedCapacity
    deleteItem/             # basic, validation
    updateItem/             # basic, conditions, validation, paths
    query/                  # basic, gsi, lsi, expressions, select, numericKeys, binaryKeys
    scan/                   # basic, validation, gsi, lsi, parallel, select, filterOperators
    batchWriteItem/         # basic, validation
    batchGetItem/           # basic, validation
    deleteTable/            # basic
    describeTable/          # basic
    listTables/             # basic
    updateTable/            # basic
  tier2/                    # ~85 tests
    transactions/           # transactWrite, transactGet
    partiql/                # executeStatement, batchExecuteStatement, executeTransaction
    ttl/                    # basic
    streams/                # basic
    tags/                   # basic
    updateTable/            # gsi
  tier3/                    # ~150 tests
    validation-ordering/    # per-operation validation error ordering
    error-messages/         # exact error message strings
    limits/                 # itemSize, batchLimits, responseSize, transactionLimits,
                            # numberPrecision, emptyValues, reservedWords
    legacy-api/             # expected, attributeUpdates, queryFilter, scanFilter, attributesToGet
```

## Shared infrastructure

- `src/client.ts` - DynamoDB and Streams client, configured from the `DYNAMODB_ENDPOINT` env var
- `src/helpers.ts` - table lifecycle, assertion helpers (`expectDynamoError`, `cleanupItems`, `waitForGsiConsistency`)
- `src/setup.ts` - global beforeAll/afterAll that creates 5 shared tables
- `src/types.ts` - `TestTableDef` and `KeyDef` types

## Generating results

```bash
# Run against a target and save JSON output
DYNAMODB_ENDPOINT=http://localhost:8000 npx vitest run --reporter=json --outputFile=results/dynamodb-local.json

# Generate the comparison table from all saved results
npm run results:table
```

## SDK blindspots

This suite uses the AWS SDK v3 (not raw HTTP), which means it can't test:

1. **Request signing validation** - the SDK always signs correctly
2. **Error wire format** - `__type` field naming, `message` vs `Message` casing
3. **Content-type handling** - the SDK always sends `application/x-amz-json-1.0`
4. **Connection-level behaviour** - HTTP headers, chunked encoding, CRC32 checks

You'd need a raw HTTP test layer using `fetch()` with `aws4` signing for those. The dynalite test suite is a good reference for that approach.

## Contributing

### Adding tests

1. Follow existing patterns in the relevant tier directory
2. Use `expectDynamoError()` for error assertions, not try/catch
3. Use `cleanupItems()` in `afterAll` for data cleanup
4. Use `ExpressionAttributeNames` for all attribute names in expressions (avoid reserved words)
5. Use `ConsistentRead: true` on all read-back assertions
6. **Test against real DynamoDB first** - if AWS fails, the test is wrong by definition

### Adding a target

1. Start the target on a port
2. Run: `DYNAMODB_ENDPOINT=http://localhost:<port> npx vitest run --reporter=json --outputFile=results/<target>.json`
3. Generate the table: `npm run results:table`
4. Submit a PR with the results JSON

### Test data

All test data must be synthetic. Don't use real names, emails, addresses, or any personally identifiable information in test fixtures.

## Operations covered

| Operation | Tier 1 | Tier 2 | Tier 3 |
|-----------|--------|--------|--------|
| PutItem | basic, conditions (incl. parens), validation, expressions, dataTypes, consumedCapacity, itemCollectionMetrics | | error messages |
| GetItem | basic, validation, projection, consumedCapacity | | |
| UpdateItem | basic, conditions (incl. parens, non-existent key branch), validation, paths | | error messages |
| DeleteItem | basic, conditions (incl. parens), validation | | error messages |
| Query | basic, GSI, LSI, expressions (incl. KeyCondition + Filter parens), select, numericKeys, binaryKeys, pagination | | error messages, validation ordering |
| Scan | basic, validation, GSI (incl. pagination), LSI (incl. pagination), parallel, select, filterOperators, filterExpression parens | | validation ordering |
| BatchWriteItem | basic, validation | | |
| BatchGetItem | basic, validation | | |
| CreateTable | basic, GSI, LSI | | error messages, validation ordering |
| DeleteTable | basic | | |
| DescribeTable | basic | | |
| ListTables | basic | | |
| UpdateTable | basic (throughput, billing mode) | GSI lifecycle | |
| TransactWriteItems | | basic, conditions (incl. parens, non-existent key branch), idempotency, cancellation | |
| TransactGetItems | | basic, validation | |
| ExecuteStatement | | INSERT, SELECT, UPDATE, DELETE, parameterised | |
| BatchExecuteStatement | | batch, partial failure | |
| ExecuteTransaction | | atomic, rollback | |
| UpdateTimeToLive | | enable, validation | |
| DescribeTimeToLive | | describe | |
| TagResource | | add, list, remove, validation | |
| DynamoDB Streams | | ListStreams, DescribeStream, GetRecords, view types | |

### Not covered (cloud-only)

- Global Tables
- Backups and Point-in-Time Recovery
- DynamoDB Accelerator (DAX)
- Kinesis Data Streams integration
- Import/Export to S3
- Table Class (Standard/Standard-IA)
- Contributor Insights

## Licence

MIT
