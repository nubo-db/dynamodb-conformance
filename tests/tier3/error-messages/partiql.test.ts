import {
  ExecuteStatementCommand,
  ExecuteTransactionCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { isUnsupportedFault } from '../../../src/infra.js'
import { declareTables, hashTableDef, absentTableName } from '../../../src/helpers.js'

declareTables(hashTableDef)

// Exact AWS strings for the PartiQL RETURNING rejections, pinned against real
// AWS (eu-west-2). Tier 2 (tests/tier2/partiql/) asserts the error shape; this
// file pins the verbatim messages. Only DELETE's invalid RETURNING variants and
// ExecuteTransaction reject the clause — BatchExecuteStatement honours it, so it
// has no rejection string to pin. See #102.
describe('PartiQL — exact error messages', { tags: ['partiql', 'data-plane', 'negative-path'] }, () => {
  let supported = true

  beforeAll(async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT * FROM "${hashTableDef.name}" WHERE pk = 'partiql-canary'`,
      }))
    } catch (e: unknown) {
      // isUnsupportedFault is the suite's definition of "not implemented", so a
      // target signalling it any recognised way (including HTTP 501) skips here
      // rather than failing every PartiQL test. UnrecognizedClientException is
      // kept alongside it: it is a credentials rejection, not an unsupported
      // fault, but it is how at least one target declines PartiQL.
      if (isUnsupportedFault(e) || (e instanceof Error && e.name === 'UnrecognizedClientException')) {
        supported = false
      }
    }
  })

  beforeEach(({ skip }) => { if (!supported) skip() })

  it('DELETE RETURNING MODIFIED OLD * — exact message', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = 'x' RETURNING MODIFIED OLD *`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid returning clause: RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE statements.',
      )
    }
  })

  it('DELETE RETURNING ALL NEW * — exact message', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = 'x' RETURNING ALL NEW *`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid returning clause: RETURNING ALL NEW *. Only RETURNING ALL OLD * is allowed in DELETE statements.',
      )
    }
  })

  it('DELETE RETURNING MODIFIED NEW * — exact message', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `DELETE FROM "${hashTableDef.name}" WHERE pk = 'x' RETURNING MODIFIED NEW *`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Invalid returning clause: RETURNING MODIFIED NEW *. Only RETURNING ALL OLD * is allowed in DELETE statements.',
      )
    }
  })

  it('ExecuteTransaction with a RETURNING member — exact message', async () => {
    try {
      await ddb.send(new ExecuteTransactionCommand({
        TransactStatements: [
          { Statement: `UPDATE "${hashTableDef.name}" SET data = 'v' WHERE pk = 'x' RETURNING ALL NEW *` },
        ],
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Validation failed in TransactStatements[0]: RETURNING clause is not supported in ExecuteTransaction.',
      )
    }
  })

  /** Run a statement and hand back the exception it raises. */
  async function rejection(Statement: string, extra: Record<string, unknown> = {}) {
    try {
      await ddb.send(new ExecuteStatementCommand({ Statement, ...extra }))
      expect.unreachable('should have thrown')
      throw new Error('unreachable')
    } catch (err) {
      if (err instanceof DynamoDBServiceException) return err
      throw err
    }
  }

  it('an ordering comparison on an unordered operand — exact message', async () => {
    const err = await rejection(
      `SELECT pk FROM "${hashTableDef.name}" WHERE pk = 'x' AND val < ?`,
      { Parameters: [{ BOOL: true }] },
    )
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe(
      'Incorrect operand type for operator or function; operator or function: <, operand type: BOOL',
    )
  })
})
