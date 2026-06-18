import {
  DescribeLimitsCommand,
  DescribeEndpointsCommand,
  type DescribeLimitsCommandOutput,
  type DescribeEndpointsCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { isUnsupportedFault } from '../../../src/infra.js'

// Account-level read operations. Probe once in beforeAll (DescribeLimits is
// rate-limited to roughly once per minute, so it must not be called twice) and
// skip on targets that do not implement the operation. Limit values are
// account-specific, so assert presence and positivity, not exact numbers.

describe('Account reads — DescribeLimits, DescribeEndpoints', { tags: ['account', 'control-plane', 'cloud-only'] }, () => {
  let limits: DescribeLimitsCommandOutput | null = null
  let endpoints: DescribeEndpointsCommandOutput | null = null
  let limitsSupported = true
  let endpointsSupported = true

  beforeAll(async () => {
    try {
      limits = await ddb.send(new DescribeLimitsCommand({}))
    } catch (e) {
      if (isUnsupportedFault(e)) limitsSupported = false
      else throw e
    }
    try {
      endpoints = await ddb.send(new DescribeEndpointsCommand({}))
    } catch (e) {
      if (isUnsupportedFault(e)) endpointsSupported = false
      else throw e
    }
  })

  it('DescribeLimits returns positive account and table capacity limits', ({ skip }) => {
    if (!limitsSupported) return skip()
    expect(limits!.AccountMaxReadCapacityUnits ?? 0).toBeGreaterThan(0)
    expect(limits!.AccountMaxWriteCapacityUnits ?? 0).toBeGreaterThan(0)
    expect(limits!.TableMaxReadCapacityUnits ?? 0).toBeGreaterThan(0)
    expect(limits!.TableMaxWriteCapacityUnits ?? 0).toBeGreaterThan(0)
  })

  it('DescribeEndpoints returns at least one endpoint with an address', ({ skip }) => {
    if (!endpointsSupported) return skip()
    expect(endpoints!.Endpoints?.length ?? 0).toBeGreaterThan(0)
    expect(endpoints!.Endpoints?.[0]?.Address).toBeTruthy()
    expect(endpoints!.Endpoints?.[0]?.CachePeriodInMinutes ?? 0).toBeGreaterThan(0)
  })
})
