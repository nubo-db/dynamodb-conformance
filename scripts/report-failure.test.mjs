import { describe, it, expect } from 'vitest'
import { collectFailures, verdictFromDrift, buildIssueBody } from './report-failure.mjs'

const report = (assertions) => ({
  testResults: [{ name: 'tests/tier3/error-messages/putItem.test.ts', assertionResults: assertions }],
})

describe('collectFailures', () => {
  it('keeps only failed assertions, with the first failure line', () => {
    const r = report([
      { status: 'passed', fullName: 'a > ok' },
      { status: 'failed', fullName: 'a > broke', failureMessages: ['Error: nope\n  at x'] },
    ])
    const fails = collectFailures(r)
    expect(fails).toHaveLength(1)
    expect(fails[0].name).toBe('a > broke')
    expect(fails[0].detail).toBe('Error: nope')
  })

  it('falls back to ancestorTitles + title when fullName is absent', () => {
    const r = report([{ status: 'failed', ancestorTitles: ['Suite', 'Case'], title: 'does X' }])
    expect(collectFailures(r)[0].name).toBe('Suite > Case > does X')
  })
})

describe('verdictFromDrift', () => {
  it('returns null when there is no usable drift data', () => {
    expect(verdictFromDrift(null)).toBeNull()
    expect(verdictFromDrift({})).toBeNull()
  })

  it('labels a clean diff as a likely flake', () => {
    const v = verdictFromDrift({ clean: true, drift: { probes: [] } })
    expect(v.label).toBe('likely-flake')
    expect(v.probes).toEqual([])
  })

  it('labels a dirty diff as confirmed drift and lists the probe ids', () => {
    const v = verdictFromDrift({ clean: false, drift: { probes: [{ id: 's_put_table_empty' }, { id: 'b_put_dup_ss' }] } })
    expect(v.label).toBe('aws-drift-confirmed')
    expect(v.probes).toEqual(['s_put_table_empty', 'b_put_dup_ss'])
  })
})

describe('buildIssueBody', () => {
  it('reports a could-not-parse body when the report is null', () => {
    expect(buildIssueBody(null, 'https://run')).toContain('could not be read or parsed')
  })

  it('lists failed tests and links the run', () => {
    const body = buildIssueBody(report([{ status: 'failed', fullName: 'a > broke' }]), 'https://run/1')
    expect(body).toContain('1 failed test')
    expect(body).toContain('a > broke')
    expect(body).toContain('https://run/1')
  })

  it('fills the triage slot with a confirmed-drift verdict and probes', () => {
    const verdict = verdictFromDrift({ clean: false, drift: { probes: [{ id: 's_put_table_empty' }] } })
    const body = buildIssueBody(report([{ status: 'failed', fullName: 'x' }]), '', verdict)
    expect(body).toContain('Verdict: AWS drift confirmed')
    expect(body).toContain('`s_put_table_empty`')
  })

  it('fills the triage slot with a flake verdict when the diff is clean', () => {
    const verdict = verdictFromDrift({ clean: true, drift: { probes: [] } })
    const body = buildIssueBody(report([{ status: 'failed', fullName: 'x' }]), '', verdict)
    expect(body).toContain('Verdict: Likely a flake')
  })

  it('falls back to the generic triage note without a verdict', () => {
    const body = buildIssueBody(report([{ status: 'failed', fullName: 'x' }]), '')
    expect(body).toContain('No drift verdict was')
  })
})
