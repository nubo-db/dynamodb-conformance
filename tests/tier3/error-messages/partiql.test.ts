import {
  ExecuteStatementCommand,
  ExecuteTransactionCommand,
  QueryCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { isUnsupportedFault } from '../../../src/infra.js'
import {
  declareTables,
  hashTableDef,
  partiqlIndexTableDef,
  PARTIQL_UNPROJECTED_ATTR,
  absentTableName,
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

  /** Run a statement and hand back the ValidationException it raises. */
  async function rejection(Statement: string, extra: Record<string, unknown> = {}) {
    try {
      await ddb.send(new ExecuteStatementCommand({ Statement, ...extra }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      return err as DynamoDBServiceException
    }
    throw new Error('unreachable')
  }

  // The trap here is the absent suffix. Query builds the same sentence and
  // appends the index name; the PartiQL surface does not, so a shared constant
  // makes one of the two wrong.
  it('a qualifier naming no index — exact message, with no name appended', async () => {
    const err = await rejection(
      `SELECT * FROM "${partiqlIndexTableDef.name}"."no-such-index" WHERE pk = 'p'`,
    )
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('The table does not have the specified index')
  })

  it('a qualifier naming the table itself — same message', async () => {
    const err = await rejection(
      `SELECT * FROM "${partiqlIndexTableDef.name}"."${partiqlIndexTableDef.name}" WHERE pk = 'p'`,
    )
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('The table does not have the specified index')
  })

  it('a three-component path — exact message', async () => {
    const err = await rejection(
      `SELECT * FROM "${partiqlIndexTableDef.name}"."gsi-all"."extra" WHERE pk = 'p'`,
    )
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('A path may contain at most 2 components in the FROM clause')
  })

  it('an empty index component — exact message', async () => {
    const err = await rejection(`SELECT * FROM "${partiqlIndexTableDef.name}"."" WHERE pk = 'p'`)
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('Path component cannot be an empty string')
  })

  // The same rejection on the table half, and it fires ahead of resolving the
  // table rather than after failing to find it.
  it('an empty table component — same message, before table resolution', async () => {
    const err = await rejection(`SELECT * FROM "" WHERE pk = 'p'`)
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('Path component cannot be an empty string')
  })

  it('ConsistentRead against a GSI qualifier — exact message', async () => {
    const err = await rejection(
      `SELECT * FROM "${partiqlIndexTableDef.name}"."gsi-all" WHERE gsiPk = 'x'`,
      { ConsistentRead: true },
    )
    expect(err.name).toBe('ValidationException')
    expect(err.message).toBe('Strongly consistent read is not supported on Global Secondary Indexes')
  })

  // The paired control. Query answers the same condition in different words, so
  // both are pinned and neither can be folded into the other.
  it('Query rejects the same condition in different words', async () => {
    try {
      await ddb.send(new QueryCommand({
        TableName: partiqlIndexTableDef.name,
        IndexName: 'gsi-all',
        KeyConditionExpression: 'gsiPk = :v',
        ExpressionAttributeValues: { ':v': { S: 'x' } },
        ConsistentRead: true,
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Consistent reads are not supported on global secondary indexes',
      )
    }
  })

  it('an index-qualified read inside a transaction — exact message', async () => {
    try {
      await ddb.send(new ExecuteTransactionCommand({
        TransactStatements: [
          { Statement: `SELECT * FROM "${partiqlIndexTableDef.name}"."gsi-all" WHERE gsiPk = 'x'` },
        ],
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Validation failed in TransactStatements[0]: Reads on indices are not supported within transactions.',
      )
    }
  })

  // Two deliberately structural. An unterminated quote is answered with the
  // envelope and nothing after it, and the detail being absent is a rendering
  // choice rather than an outcome; a trailing space in a string literal is also
  // the kind of thing a formatter eats silently. ResourceNotFoundException gets
  // the same treatment because an implementation naming the table is adding
  // detail, not giving a different answer.
  it('an unterminated quoted name — envelope, asserted structurally', async () => {
    const err = await rejection(`SELECT * FROM "${partiqlIndexTableDef.name}`)
    expect(err.name).toBe('ValidationException')
    expect(err.message).toContain("Statement wasn't well formed, can't be processed")
  })

  it('a table that does not exist — ResourceNotFoundException, asserted structurally', async () => {
    const err = await rejection(`SELECT * FROM "${absentTableName('em_partiql_missing')}" WHERE pk = 'p'`)
    expect(err.name).toBe('ResourceNotFoundException')
    expect(err.message).toContain('Requested resource not found')
  })

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
