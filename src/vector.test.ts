import { DynamoDBServiceException } from '@aws-sdk/client-dynamodb'

// Unit tests for the vector probes' fault classification and the waiters'
// ceiling behaviour, with the client mocked the way the sibling module tests
// do. The probes memoise across a run, so every case reloads the module.

const sendMock = vi.fn()

vi.mock('./client.js', () => ({
  ddb: { send: (cmd: unknown) => sendMock(cmd) },
  ddbStreams: {},
}))

function named(cmd: unknown): string {
  return (cmd as { constructor: { name: string } }).constructor.name
}

function serviceError(name: string, message: string): DynamoDBServiceException {
  return new DynamoDBServiceException({
    name,
    message,
    $fault: 'client',
    $metadata: { httpStatusCode: 400 },
  })
}

async function loadVector() {
  vi.resetModules()
  return await import('./vector.js')
}

beforeEach(() => {
  sendMock.mockReset()
})

describe('supportsSearchVectors', () => {
  it('classifies an unsupported fault as not implemented, memoised', async () => {
    const vector = await loadVector()
    sendMock.mockRejectedValue(serviceError('UnknownOperationException', 'Unknown operation.'))
    expect(await vector.supportsSearchVectors()).toBe(false)
    expect(await vector.supportsSearchVectors()).toBe(false)
    expect(sendMock).toHaveBeenCalledTimes(1)
  })

  it('classifies a real rejection as implemented', async () => {
    const vector = await loadVector()
    sendMock.mockRejectedValue(
      serviceError('ResourceNotFoundException', 'Requested resource not found'),
    )
    expect(await vector.supportsSearchVectors()).toBe(true)
  })
})

describe('supportsVectorIndexes', () => {
  // The probe's cleanup path (DescribeTable wait + DeleteTable) runs in a
  // finally; answering not-found keeps it silent without a created table.
  function answerCleanup(cmd: unknown): Promise<unknown> | null {
    if (named(cmd) === 'DeleteTableCommand') {
      return Promise.reject(serviceError('ResourceNotFoundException', 'not found'))
    }
    return null
  }

  it('classifies an unsupported fault as not implemented', async () => {
    const vector = await loadVector()
    sendMock.mockImplementation((cmd) => {
      const cleanup = answerCleanup(cmd)
      if (cleanup) return cleanup
      if (named(cmd) === 'DescribeTableCommand') {
        return Promise.reject(serviceError('ResourceNotFoundException', 'not found'))
      }
      return Promise.reject(serviceError('UnknownOperationException', 'Unknown operation.'))
    })
    expect(await vector.supportsVectorIndexes()).toBe(false)
  })

  it('classifies a ValidationException as not implemented (scope, not divergence)', async () => {
    const vector = await loadVector()
    sendMock.mockImplementation((cmd) => {
      const cleanup = answerCleanup(cmd)
      if (cleanup) return cleanup
      if (named(cmd) === 'DescribeTableCommand') {
        return Promise.reject(serviceError('ResourceNotFoundException', 'not found'))
      }
      return Promise.reject(serviceError('ValidationException', 'Unknown parameter'))
    })
    expect(await vector.supportsVectorIndexes()).toBe(false)
  })

  it('classifies acceptance without reflection as not implemented', async () => {
    const vector = await loadVector()
    sendMock.mockImplementation((cmd) => {
      const cleanup = answerCleanup(cmd)
      if (cleanup) return cleanup
      if (named(cmd) === 'CreateTableCommand') return Promise.resolve({})
      if (named(cmd) === 'DescribeTableCommand') {
        return Promise.resolve({ Table: { TableStatus: 'ACTIVE' } })
      }
      return Promise.resolve({})
    })
    expect(await vector.supportsVectorIndexes()).toBe(false)
  })

  it('classifies acceptance with reflection as implemented', async () => {
    const vector = await loadVector()
    sendMock.mockImplementation((cmd) => {
      const cleanup = answerCleanup(cmd)
      if (cleanup) return cleanup
      if (named(cmd) === 'CreateTableCommand') return Promise.resolve({})
      if (named(cmd) === 'DescribeTableCommand') {
        return Promise.resolve({
          Table: {
            TableStatus: 'ACTIVE',
            VectorIndexes: [{ IndexName: 'probe-index', IndexStatus: 'ACTIVE' }],
          },
        })
      }
      return Promise.resolve({})
    })
    expect(await vector.supportsVectorIndexes()).toBe(true)
  })

  it('errs on implemented for faults that are not answers about support', async () => {
    const vector = await loadVector()
    sendMock.mockImplementation((cmd) => {
      const cleanup = answerCleanup(cmd)
      if (cleanup) return cleanup
      if (named(cmd) === 'DescribeTableCommand') {
        return Promise.reject(serviceError('ResourceNotFoundException', 'not found'))
      }
      return Promise.reject(Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' }))
    })
    expect(await vector.supportsVectorIndexes()).toBe(true)
  })
})

describe('waitForVectorIndexActive', () => {
  it('resolves once the index is ACTIVE and not backfilling', async () => {
    const vector = await loadVector()
    sendMock.mockResolvedValue({
      Table: { VectorIndexes: [{ IndexName: 'vix', IndexStatus: 'ACTIVE' }] },
    })
    await expect(vector.waitForVectorIndexActive('t', 'vix')).resolves.toBeUndefined()
  })

  it('types a ceiling expiry as indeterminate, never a divergence', async () => {
    const vector = await loadVector()
    sendMock.mockResolvedValue({
      Table: { VectorIndexes: [{ IndexName: 'vix', IndexStatus: 'CREATING' }] },
    })
    const err = (await vector
      .waitForVectorIndexActive('t', 'vix', { timeoutMs: 30 })
      .then(() => null)
      .catch((e: unknown) => e)) as { name?: string; reason?: string } | null
    // Module reloads mint a fresh IndeterminateError class, so the check is
    // structural rather than instanceof.
    expect(err?.name).toBe('IndeterminateError')
    expect(err?.reason).toBe('vector-index-timeout')
  })
})

describe('waitForVectorSearchable', () => {
  it('resolves once the expected count is visible', async () => {
    const vector = await loadVector()
    sendMock.mockResolvedValue({ SearchResults: [{}, {}] })
    await expect(
      vector.waitForVectorSearchable({
        tableName: 't',
        indexName: 'vix',
        searchVector: [{ N: '1' }],
        expectedCount: 2,
      }),
    ).resolves.toBeUndefined()
  })

  it('types a ceiling expiry as indeterminate', async () => {
    const vector = await loadVector()
    sendMock.mockResolvedValue({ SearchResults: [] })
    const err = (await vector
      .waitForVectorSearchable({
        tableName: 't',
        indexName: 'vix',
        searchVector: [{ N: '1' }],
        expectedCount: 1,
        timeoutMs: 30,
      })
      .then(() => null)
      .catch((e: unknown) => e)) as { name?: string; reason?: string } | null
    expect(err?.name).toBe('IndeterminateError')
    expect(err?.reason).toBe('vector-consistency-timeout')
  })
})
