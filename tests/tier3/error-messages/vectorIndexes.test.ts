import {
  CreateTableCommand,
  DynamoDBServiceException,
  type CreateTableCommandInput,
  type VectorIndex,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import { skipUnlessVectorIndexes } from '../../../src/vector.js'

// Exact service-layer messages for vector index creation, characterised
// against real DynamoDB in eu-west-2 (2026-08-11, issue #125). Every case is
// a synchronous CreateTable rejection, so no table exists afterwards; if a
// target unexpectedly accepts one, the created table is removed before the
// assertion fails.

function vix(over: Partial<VectorIndex> = {}): VectorIndex {
  return {
    IndexName: 'vix',
    VectorAttribute: { AttributeName: 'embedding' },
    Dimensions: 3,
    DistanceFunction: 'COSINE',
    Projection: { ProjectionType: 'KEYS_ONLY' },
    ...over,
  }
}

function base(name: string): CreateTableCommandInput {
  return {
    TableName: name,
    AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
    KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
    BillingMode: 'PAY_PER_REQUEST',
  }
}

async function expectExactRejection(
  input: CreateTableCommandInput,
  message: string,
): Promise<void> {
  try {
    await ddb.send(new CreateTableCommand(input))
    await deleteTable(input.TableName!)
    expect.unreachable('should have thrown')
  } catch (err) {
    expect(err).toBeInstanceOf(DynamoDBServiceException)
    expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    expect((err as DynamoDBServiceException).message).toBe(message)
  }
}

describe('CreateTable vector indexes — exact error messages', { tags: ['create-table', 'control-plane', 'vector', 'negative-path'] }, () => {
  skipUnlessVectorIndexes()

  it('vector index on a provisioned table', async () => {
    const input = base(uniqueTableName('vec_msg'))
    delete input.BillingMode
    input.ProvisionedThroughput = { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
    input.VectorIndexes = [vix()]
    await expectExactRejection(
      input,
      'One or more parameter values were invalid: Vector indexes are only supported for PAY_PER_REQUEST tables',
    )
  })

  it('SearchSchema attribute missing from AttributeDefinitions', async () => {
    const input = base(uniqueTableName('vec_msg'))
    input.VectorIndexes = [
      vix({ SearchSchema: [{ AttributeName: 'tenant', SearchSchemaElementType: 'HASH' }] }),
    ]
    await expectExactRejection(
      input,
      'One or more parameter values were invalid: One element in SearchSchema is not defined in attribute definitions',
    )
  })

  it('Dimensions above the 4096 maximum', async () => {
    const input = base(uniqueTableName('vec_msg'))
    input.VectorIndexes = [vix({ Dimensions: 4097 })]
    await expectExactRejection(
      input,
      'One or more parameter values were invalid: Number of dimensions must be between 1 and 4096 inclusive.',
    )
  })

  it('more vector indexes than the per-table limit', async () => {
    const input = base(uniqueTableName('vec_msg'))
    input.VectorIndexes = Array.from({ length: 6 }, (_, i) => vix({ IndexName: `vix-${i}` }))
    await expectExactRejection(
      input,
      'One or more parameter values were invalid: VectorIndex count exceeds the per-table limit of 5',
    )
  })

  it('two indexes on one attribute with different Dimensions', async () => {
    const input = base(uniqueTableName('vec_msg'))
    input.VectorIndexes = [vix(), vix({ IndexName: 'vix2', Dimensions: 4 })]
    await expectExactRejection(
      input,
      "One or more parameter values were invalid: Conflicting attribute definition for 'embedding'. All VectorIndexes on the same vector attribute must use the same dimensions.",
    )
  })
})
