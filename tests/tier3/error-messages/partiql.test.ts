import {
  ExecuteStatementCommand,
  ExecuteTransactionCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { isUnsupportedFault } from '../../../src/infra.js'
import {
  declareTables,
  hashTableDef,
  partiqlIndexTableDef,
  PARTIQL_UNPROJECTED_ATTR,
} from '../../../src/helpers.js'

declareTables(hashTableDef, partiqlIndexTableDef)

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

  // The index rejections. Note the wording: `Secondary index`, with neither
  // `Global` nor `Local` in front, on both kinds. Query builds its own string
  // for the same condition, so a shared constant would make one of them wrong.
  it('unprojected filter attribute on a GSI — exact message', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT pk FROM "${partiqlIndexTableDef.name}"."gsi-inc" WHERE gsiPk2 = 'y' AND ${PARTIQL_UNPROJECTED_ATTR} = 'np1'`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `One or more parameter values were invalid: Secondary index gsi-inc does not project one or more filter attributes: [${PARTIQL_UNPROJECTED_ATTR}]`,
      )
    }
  })

  it('unprojected filter attribute on an LSI — same wording, no Local prefix', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT pk FROM "${partiqlIndexTableDef.name}"."lsi-keys" WHERE pk = 'p' AND ${PARTIQL_UNPROJECTED_ATTR} = 'np1'`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `One or more parameter values were invalid: Secondary index lsi-keys does not project one or more filter attributes: [${PARTIQL_UNPROJECTED_ATTR}]`,
      )
    }
  })

  // The projection rejection is worded differently from the filter one, and this
  // half does name the index kind.
  it('unprojected projection attribute on a GSI — exact message', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${partiqlIndexTableDef.name}"."gsi-inc" WHERE gsiPk2 = 'y'`,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        `One or more parameter values were invalid: Global secondary index gsi-inc does not project [${PARTIQL_UNPROJECTED_ATTR}]`,
      )
    }
  })
})
