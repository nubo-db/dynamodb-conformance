// Raw-message drift between two capture blocks.
//
// A "block" is one region's slice of a capture document, the shape that
// scripts/capture-validation-messages.mjs emits per region:
//
//   { probes: [{ id, name, message, n, fields, ... }], nullRoundTrip: {...} }
//
// Drift is a difference in the *raw* values a probe returned - the error type
// (`name`), the full `message`, the `N validation error detected` count, or the
// named `fields` list. This is deliberately independent of whether the Tier 3
// suite would pass or fail: a probe whose error type and field are unchanged but
// whose prose was reworded still *passes* the tolerant (type + field +
// constraint) assertions, yet its raw message drifted. That passing-but-different
// case is exactly what a pass/fail signal is blind to and why drift is measured
// by diffing captured messages, never by re-running the suite.
//
// Pure and dependency-free so it can be unit-tested directly (no AWS, no
// network) and mirrored in the website's lens derivation.

/** The raw fields that count as drift when they move. */
function pick(probe) {
  return {
    name: probe?.name ?? null,
    message: probe?.message ?? null,
    n: probe?.n ?? null,
    fields: probe?.fields ?? [],
  }
}

function sameFields(a = [], b = []) {
  if (a.length !== b.length) return false
  return a.every((v, i) => v === b[i])
}

/** Diff one probe against its baseline. Returns null when nothing moved. */
export function diffProbe(baseline, observed) {
  const changed = []
  if (baseline.name !== observed.name) changed.push('name')
  if (baseline.message !== observed.message) changed.push('message')
  if (baseline.n !== observed.n) changed.push('n')
  if (!sameFields(baseline.fields, observed.fields)) changed.push('fields')
  if (changed.length === 0) return null
  // Only the prose moved - type, field and count are intact - so a tolerant
  // assertion still passes while the raw wording drifted.
  const passingButDifferent = changed.length === 1 && changed[0] === 'message'
  return {
    id: baseline.id ?? observed.id,
    changed,
    passingButDifferent,
    baseline: pick(baseline),
    observed: pick(observed),
  }
}

function diffNullRoundTrip(baseline, observed) {
  const b = JSON.stringify(baseline ?? null)
  const o = JSON.stringify(observed ?? null)
  if (b === o) return null
  return { changed: ['nullRoundTrip'], baseline: baseline ?? null, observed: observed ?? null }
}

function indexById(probes = []) {
  const map = new Map()
  for (const p of probes) if (p && p.id != null) map.set(p.id, p)
  return map
}

/**
 * Diff an observed block against a baseline block. Returns the probe-level
 * divergences (probes present in only one side are reported as added/removed)
 * and any nullRoundTrip divergence. An empty `probes` array and a null
 * `nullRoundTrip` means the two blocks match.
 */
export function diffCaptures(baseline, observed) {
  const base = indexById(baseline?.probes)
  const obs = indexById(observed?.probes)
  const ids = [...new Set([...base.keys(), ...obs.keys()])]
  const probes = []
  for (const id of ids) {
    const b = base.get(id)
    const o = obs.get(id)
    if (!b) {
      probes.push({ id, changed: ['added'], passingButDifferent: false, observed: pick(o) })
      continue
    }
    if (!o) {
      probes.push({ id, changed: ['removed'], passingButDifferent: false, baseline: pick(b) })
      continue
    }
    const d = diffProbe(b, o)
    if (d) probes.push(d)
  }
  return { probes, nullRoundTrip: diffNullRoundTrip(baseline?.nullRoundTrip, observed?.nullRoundTrip) }
}

/** True when a diff result carries no divergence at all. */
export function isClean(diff) {
  return (diff?.probes?.length ?? 0) === 0 && !diff?.nullRoundTrip
}

/**
 * Compare every non-baseline region in a capture document against the
 * baseline region within the same document. Used for the cross-region lens:
 * "which regions differ from eu-west-2 right now". Returns
 * { baselineRegion, regions: { <region>: <diffCaptures result> } }.
 */
export function diffRegions(captureDoc, baselineRegion = 'eu-west-2') {
  const regions = captureDoc?.regions ?? {}
  const base = regions[baselineRegion]
  const out = {}
  for (const [region, block] of Object.entries(regions)) {
    if (region === baselineRegion) continue
    out[region] = diffCaptures(base, block)
  }
  return { baselineRegion, regions: out }
}
