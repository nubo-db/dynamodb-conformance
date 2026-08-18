import {
  ExecuteStatementCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { absentTableName } from '../../../src/helpers.js'

// Which check answers first when a PartiQL statement has more than one problem.
// The wording can drift; the ordering should not, so these use `toContain`.
describe('PartiQL — validation ordering', { tags: ['partiql', 'data-plane', 'negative-path'] }, () => {
  // The operand-type check is a property of the statement, so it answers without
  // the table being resolved at all. An engine evaluating it as a comparison
  // outcome reaches the table first and reports the table missing instead.
  it('the operand-type check fires ahead of table resolution', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT pk FROM "${absentTableName('vo_partiql_missing')}" WHERE pk = ? AND val < ?`,
        Parameters: [{ S: 'x' }, { BOOL: true }],
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain('Incorrect operand type')
    }
  })

  // The same table, named correctly, still reports itself missing. Without this
  // the case above proves only that something was rejected.
  it('a well-formed statement on the same table reports it missing', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT pk FROM "${absentTableName('vo_partiql_missing')}" WHERE pk = ?`,
        Parameters: [{ S: 'x' }],
      }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ResourceNotFoundException')
    }
  })

  // The empty-component check also answers before the table is looked up, and
  // it applies to the table half of the path as much as the index half.
  it('an empty path component fires ahead of table resolution', async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({ Statement: `SELECT * FROM "" WHERE pk = 'x'` }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toContain('Path component cannot be an empty string')
    }
  })
})
