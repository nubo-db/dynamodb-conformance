// Shape the suite's split registry (registry/splits.json) for the explainer.
//
// A split is a behaviour where real DynamoDB regions give different definite
// answers, with what each region actually returned. This turns one split into
// cohorts - regions grouped by the answer they gave - so the evidence reads as
// "these regions say X, those say Y" rather than a wall of per-region rows.

const PINNED = "eu-west-2";

const esc = (s) =>
  String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

// A stable key for a region's answer, so regions that returned the same thing
// group together. Two answers match when their outcome and their error (name +
// message) or accepted detail are identical.
function answerKey(a) {
  if (a?.error) return `err|${a.error.name}|${a.error.message}`;
  if (a?.detail) return `ok|${a.detail}`;
  return `${a?.outcome ?? "?"}`;
}

export function shapeSplit(s, pinned = PINNED) {
  const byAnswer = new Map();
  for (const [region, ans] of Object.entries(s.regions ?? {})) {
    const key = answerKey(ans);
    if (!byAnswer.has(key)) byAnswer.set(key, { outcome: ans.outcome ?? "?", error: ans.error ?? null, detail: ans.detail ?? null, regions: [] });
    byAnswer.get(key).regions.push(region);
  }
  const groups = [...byAnswer.values()]
    .map((g) => ({ ...g, regions: g.regions.sort(), count: g.regions.length, hasPinned: g.regions.includes(pinned) }))
    .sort((a, b) => b.count - a.count);
  // The test identity travels with the row. It is what makes a split a fact
  // about a specific assertion rather than a paragraph, and the A+ premise
  // check matches on it - without it here that check has to read the registry
  // file directly, which is the wrong source on a live build.
  return { id: s.id, test: s.test ?? null, behaviour: s.behaviour ?? "", firstObserved: s.firstObserved ?? null, pinned: s.pinned ?? pinned, groups };
}

export function buildSplitsModel(raw, { pinned = PINNED } = {}) {
  if (!raw || !Array.isArray(raw.splits) || raw.splits.length === 0) {
    return { available: false, splits: [], count: 0, featured: null };
  }
  const splits = raw.splits.map((s) => shapeSplit(s, pinned));
  // Feature the split whose cohorts differ most visibly - the most distinct
  // error kinds - so the evidence reads as genuinely different answers rather
  // than near-identical ones.
  const distinctKinds = (s) => new Set(s.groups.map((g) => g.error?.name ?? g.outcome)).size;
  const featured = [...splits].sort((a, b) => distinctKinds(b) - distinctKinds(a))[0];
  return { available: true, splits, count: splits.length, featured };
}

/**
 * The observed regions a split does not account for, by name.
 *
 * The cohorts add up to fewer regions than the board says it scores, and the
 * gap has two causes that look identical on the page: a region with no definite
 * recorded answer when the evidence was captured, and a region named in the row
 * that has since been dropped. Left as bare cohort counts the reader is invited
 * to subtract and find regions missing.
 */
export function splitCoverage(split, observed = []) {
  // An empty observed set means the region overlay is unavailable, not that
  // every region in the row has dropped out. Nothing to compare against.
  if (!split || !observed.length) return null;
  const named = new Set(split.groups.flatMap((g) => g.regions));
  const unrecorded = observed.filter((r) => !named.has(r));
  return {
    observed: observed.length,
    accounted: observed.length - unrecorded.length,
    unrecorded,
    // Named in the row but no longer scored, so the cohort counts above can
    // exceed what the split accounts for today.
    departed: [...named].filter((r) => !observed.includes(r)),
  };
}

/** One line of prose for the arithmetic above, or "" when it all adds up. */
export function splitCoverageNote(split, observed = []) {
  const c = splitCoverage(split, observed);
  if (!c || (!c.unrecorded.length && !c.departed.length)) return "";
  const parts = [];
  if (c.unrecorded.length) {
    parts.push(
      `${c.unrecorded.length} of the ${c.observed} observed regions had no definite recorded answer for this behaviour when the evidence was captured (${c.unrecorded.join(", ")})`,
    );
  }
  if (c.departed.length) {
    parts.push(
      `${c.departed.join(", ")} answered at capture but ${c.departed.length === 1 ? "has" : "have"} since dropped out of scoring`,
    );
  }
  return `The cohorts account for ${c.accounted} of them: ${parts.join("; ")}.`;
}

// Render one split's cohorts as HTML for the explainer. WebC can't nest the
// cohorts-then-regions loop, so this is built here like the other grid helpers.
export function renderSplitEvidence(split) {
  if (!split || split.groups.length === 0) return "";
  return split.groups
    .map((g) => {
      const heading = g.error ? esc(g.error.name) : g.outcome === "accepted" ? "Accepted" : esc(g.outcome);
      const detail = g.error ? esc(g.error.message) : g.detail ? esc(g.detail) : "";
      const regions = g.regions
        .map((r) =>
          r === split.pinned
            ? `<span class="font-medium text-zinc-700 dark:text-zinc-200">${esc(r)} <span class="text-zinc-400 dark:text-zinc-500">· baseline</span></span>`
            : `<span>${esc(r)}</span>`,
        )
        .join("");
      return `
      <div class="rounded-xl border border-zinc-200 dark:border-white/10 bg-zinc-50/70 dark:bg-white/[0.03] p-4">
        <div class="flex items-baseline justify-between gap-3 mb-2">
          <span class="font-mono text-sm font-semibold text-zinc-900 dark:text-zinc-100">${heading}</span>
          <span class="text-xs text-zinc-500 dark:text-zinc-400 tnum shrink-0">${g.count} ${g.count === 1 ? "region" : "regions"}</span>
        </div>
        <p class="font-mono text-xs text-zinc-600 dark:text-zinc-300 whitespace-pre-wrap break-words mb-3">${detail}</p>
        <div class="flex flex-wrap gap-x-3 gap-y-1 text-xs font-mono text-zinc-500 dark:text-zinc-400">${regions}</div>
      </div>`;
    })
    .join("");
}
