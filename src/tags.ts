// Canonical vocabulary of vitest test tags for the conformance suite.
//
// Tags are an axis orthogonal to the tier directories: a test lives in exactly
// one tier but can carry several tags, so you can run or exclude tests by
// capability (`vitest --tags-filter="!partiql"`) and by cross-cutting axis
// (`vitest --tags-filter="data-plane and !cloud-only"`). Tiers still answer
// "how strict"; tags answer "which capability".
//
// This array is the single source of truth. vitest.config.ts feeds it to
// `test.tags`, so `strictTags` (on by default) rejects any tag not declared
// here; the tag-coverage guard checks every test against it; and the README
// table mirrors it. Declare a tag here before applying it to a test.

export interface TagDef {
  readonly name: string
  readonly description: string
}

// Operation tags — data plane. Operations that read or write items.
const DATA_PLANE_OPS: readonly TagDef[] = [
  { name: 'put-item', description: 'PutItem' },
  { name: 'get-item', description: 'GetItem' },
  { name: 'update-item', description: 'UpdateItem' },
  { name: 'delete-item', description: 'DeleteItem' },
  { name: 'query', description: 'Query' },
  { name: 'scan', description: 'Scan' },
  { name: 'batch', description: 'BatchGetItem and BatchWriteItem' },
  { name: 'transactions', description: 'TransactWriteItems and TransactGetItems' },
  {
    name: 'partiql',
    description: 'PartiQL: ExecuteStatement, BatchExecuteStatement, ExecuteTransaction',
  },
  { name: 'search-vectors', description: 'SearchVectors' },
]

// Operation tags — control plane. Operations that manage tables, indexes, or
// table-level features.
const CONTROL_PLANE_OPS: readonly TagDef[] = [
  { name: 'create-table', description: 'CreateTable' },
  { name: 'update-table', description: 'UpdateTable' },
  { name: 'delete-table', description: 'DeleteTable' },
  { name: 'describe-table', description: 'DescribeTable' },
  { name: 'list-tables', description: 'ListTables' },
  { name: 'ttl', description: 'UpdateTimeToLive and DescribeTimeToLive' },
  { name: 'streams', description: 'DynamoDB Streams (ListStreams, DescribeStream, GetRecords)' },
  { name: 'resource-tags', description: 'TagResource, UntagResource, ListTagsOfResource' },
  { name: 'backups', description: 'On-demand backups and point-in-time recovery' },
  { name: 'export-import', description: 'ExportTableToPointInTime and ImportTable' },
  { name: 'kinesis', description: 'Kinesis streaming destinations' },
  { name: 'contributor-insights', description: 'UpdateContributorInsights' },
  { name: 'resource-policy', description: 'PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy' },
  { name: 'account', description: 'Account-level reads: DescribeLimits, DescribeEndpoints' },
]

// Cross-cutting tags. Axes that span operations.
const CROSS_CUTTING: readonly TagDef[] = [
  { name: 'data-plane', description: 'Reads or writes items' },
  { name: 'control-plane', description: 'Manages tables, indexes, or table-level features' },
  {
    name: 'cloud-only',
    description:
      'No emulator implements it: needs real AWS infrastructure, another AWS service, or account/region context',
  },
  { name: 'gsi', description: 'Exercises Global Secondary Indexes' },
  { name: 'lsi', description: 'Exercises Local Secondary Indexes' },
  { name: 'vector', description: 'Exercises vector indexes or vector search' },
  {
    name: 'legacy',
    description:
      'Deprecated request parameters (AttributeUpdates, QueryFilter, ScanFilter, Expected, AttributesToGet)',
  },
  {
    name: 'slow',
    description:
      'Long-running against real AWS; the set test:gating excludes (GSI lifecycle, export/import, Kinesis)',
  },
  {
    name: 'negative-path',
    description:
      'Asserts only rejections: every case expects a validation error, conditional-check failure, or transaction cancellation, with no accepted operation as its outcome',
  },
]

// The full registry, in declaration order.
export const TAGS: readonly TagDef[] = [
  ...DATA_PLANE_OPS,
  ...CONTROL_PLANE_OPS,
  ...CROSS_CUTTING,
]

// Every declared tag name, for the coverage guard.
export const TAG_NAMES: ReadonlySet<string> = new Set(TAGS.map((t) => t.name))

// The two plane tags partition the suite: every test carries exactly one.
export const PLANE_TAGS = ['data-plane', 'control-plane'] as const
