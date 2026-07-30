// Build the area-by-target support grid for the latest run, from the model's
// allAreas axis and each target's per-area state. Columns are targets
// (DynamoDB first, then by standing). Each cell is one of: supported, partial,
// unsupported, failing, or "n/a" (the area isn't in that target's results).
//
// Returns two views of the same data:
//   `items`    - a single flat list (corner, column headers, then per tier a
//                header item followed by a row-head + one cell per target) for
//                the wide desktop grid. WebC can't nest a webc:for over a
//                property of an outer loop variable, so a flat list is the
//                reliable shape for that one-loop template.
//   `sections` - a nested tier → row → cell shape for the mobile card layout,
//                which a render helper (renderSupportCards) turns into HTML,
//                since the same WebC nesting limit rules out a card template.
// `ncols` is the column count (targets + the operation column).

const TIER_LABEL = { tier1: "Tier 1 - Core", tier2: "Tier 2 - Complete", tier3: "Tier 3 - Strict" };
const TIER_SHORT = { tier1: "Tier 1", tier2: "Tier 2", tier3: "Tier 3" };

export function buildMatrix(model) {
  // The baseline is left out. Every one of its cells is supported by
  // definition, so the column cost width and carried no information.
  const targets = (model.targets || []).filter((slug) => !model.perTarget[slug]?.baseline).map((slug) => ({
    slug,
    display: model.perTarget[slug]?.display ?? slug,
    // The version each support figure was measured against - surfaced so a
    // reader can tell whether the data predates a target's own newer release.
    version: model.perTarget[slug]?.currentVersion ?? "-",
  }));

  // Per target, the full per-area tally keyed by area - the cell carries the
  // pass/fail/skip counts too so the tooltip can say why a cell is partial.
  const areaByTarget = {};
  for (const { slug } of targets) {
    const map = {};
    for (const a of model.perTarget[slug]?.areas || []) map[a.key] = a;
    areaByTarget[slug] = map;
  }

  // The same operation can be exercised in more than one tier (e.g. updateTable
  // in Tier 1 and Tier 2). When a group spans tiers its row head carries a tier
  // qualifier, so a reader sees why the operation appears twice.
  const tiersByGroup = {};
  for (const a of model.allAreas || []) (tiersByGroup[a.group] ||= new Set()).add(a.tier);

  const items = [{ type: "corner" }];
  for (const t of targets) items.push({ type: "col", slug: t.slug, display: t.display, version: t.version });

  const sections = [];
  for (const tier of ["tier1", "tier2", "tier3"]) {
    const areas = (model.allAreas || []).filter((a) => a.tier === tier);
    if (areas.length === 0) continue;
    items.push({ type: "tier", tier, label: TIER_LABEL[tier] || tier });
    const rows = [];
    for (const a of areas) {
      const qualifier = tiersByGroup[a.group].size > 1 ? TIER_SHORT[a.tier] ?? a.tier : null;
      items.push({ type: "rowhead", group: a.group, tier: a.tier, qualifier });
      const cells = [];
      for (const t of targets) {
        const cell = areaByTarget[t.slug][a.key];
        const c = {
          slug: t.slug,
          display: t.display,
          version: t.version,
          group: a.group,
          state: cell?.state ?? "n/a",
          passed: cell?.passed ?? 0,
          failed: cell?.failed ?? 0,
          skipped: cell?.skipped ?? 0,
        };
        items.push({ type: "cell", ...c });
        cells.push(c);
      }
      rows.push({ group: a.group, tier: a.tier, qualifier, cells });
    }
    sections.push({ tier, label: TIER_LABEL[tier] || tier, rows });
  }

  return { targets, ncols: targets.length + 1, items, sections };
}

// The cell glyph, colour and spoken label per state - shared by the desktop
// grid (via its own setup script) and the mobile cards below. Colour never
// carries meaning alone: an sr-only label states the support level in words.
const STATE = {
  supported: { glyph: "✓", cls: "text-pass-700 dark:text-pass-400", label: "supported" },
  partial: { glyph: "◑", cls: "text-partial-700 dark:text-partial-400", label: "partially supported" },
  failing: { glyph: "✗", cls: "text-fail-700 dark:text-fail-400", label: "failing" },
  unsupported: { glyph: "–", cls: "text-zinc-500 dark:text-zinc-400", label: "unsupported" },
};
const STATE_FALLBACK = { glyph: "·", cls: "text-zinc-300 dark:text-zinc-700", label: "not tested" };

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

function cellCounts(cell) {
  const parts = [];
  if (cell.passed) parts.push(cell.passed + " pass");
  if (cell.failed) parts.push(cell.failed + " fail");
  if (cell.skipped) parts.push(cell.skipped + " skip");
  return parts.join(", ");
}

// Mobile rendering of the support grid: a wide table is unreadable on a phone,
// so the matrix folds into one card per operation, each listing every target
// with its support glyph in a two-column layout. Grouping is by tier, matching
// the desktop sections; the tier qualifier the desktop row heads carry is
// redundant here because each operation already sits under its tier heading.
export function renderSupportCards(matrix) {
  return matrix.sections
    .map(
      (sec) => `
    <section>
      <h2 class="text-xs uppercase tracking-wide font-semibold text-zinc-500 dark:text-zinc-400 mb-3">${esc(sec.label)}</h2>
      <ul class="space-y-3">
        ${sec.rows
          .map((row) => {
            const cells = row.cells
              .map((c) => {
                const s = STATE[c.state] || STATE_FALLBACK;
                const counts = cellCounts(c);
                const describe = `${c.display} ${row.group}: ${s.label}${counts ? ` (${counts})` : ""}`;
                return `
                <div class="flex items-center justify-between gap-2 min-w-0">
                  <dt class="min-w-0">
                    <span class="block text-sm text-zinc-600 dark:text-zinc-300 truncate">${esc(c.display)}</span>
                    <span class="block font-mono text-[0.7rem] text-zinc-500 dark:text-zinc-400 truncate" title="${esc(c.version)}">${esc(c.version)}</span>
                  </dt>
                  <dd class="shrink-0 text-base font-bold leading-none ${s.cls}" title="${esc(describe)}">
                    <span aria-hidden="true">${s.glyph}</span>
                    <span class="sr-only">${esc(describe)}</span>
                  </dd>
                </div>`;
              })
              .join("");
            return `
          <li class="rounded-xl border border-zinc-200 dark:border-white/10 bg-zinc-50/70 dark:bg-white/[0.03] overflow-hidden">
            <div class="px-4 py-2.5 border-b border-zinc-200 dark:border-white/10">
              <span class="font-mono text-sm text-zinc-900 dark:text-zinc-100">${esc(row.group)}</span>
            </div>
            <!-- More columns as the card widens. These now stand in for the
                 table right up to lg, and at two columns a 900px card left each
                 glyph stranded half a card away from the name it belongs to. -->
            <dl class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-x-5 gap-y-2 px-4 py-3">
              ${cells}
            </dl>
          </li>`;
          })
          .join("")}
      </ul>
    </section>`,
    )
    .join("");
}

// A single target's per-operation scorecard: every operation area it touches,
// grouped by tier, each with its support state, divergence and coverage. This is
// the per-operation map the tier headline rolls up, so a reader can see exactly
// which operations a target is weak on, not just which tier. Built here (not in
// a template) for the same WebC nesting reason as the support cards.
//
// The figures are divergence over each area's whole size, the same axis as the
// tier above and the headline above that. They were a pass rate over what the
// area attempted, which on a page whose every other percentage had inverted
// left the most detailed table on it reading in the opposite direction.
export function renderTargetOperations(areas) {
  if (!areas || areas.length === 0) return "";
  const byTier = { tier1: [], tier2: [], tier3: [] };
  for (const a of areas) if (byTier[a.tier]) byTier[a.tier].push(a);
  return ["tier1", "tier2", "tier3"]
    .filter((t) => byTier[t].length)
    .map((t) => {
      const rows = byTier[t]
        .slice()
        .sort((a, b) => a.group.localeCompare(b.group))
        .map((a) => {
          const s = STATE[a.state] || STATE_FALLBACK;
          const impl = a.passed + a.failed;
          const rate = impl === 0 || !a.total ? "n/a" : `${((a.failed / a.total) * 100).toFixed(1)}%`;
          const cover = !a.total ? "n/a" : `${((impl / a.total) * 100).toFixed(1)}%`;
          const counts = cellCounts(a);
          // An area a target implements none of has no divergence to read out,
          // so the description says that rather than "diverges on n/a".
          const figures = rate === "n/a" ? `implements none of it` : `diverges on ${rate} of it, covers ${cover}`;
          const describe = `${a.group}: ${s.label}, ${figures}${counts ? ` (${counts})` : ""}`;
          return `
          <li class="flex items-center justify-between gap-3 py-1.5">
            <span class="flex items-center gap-2 min-w-0">
              <span class="text-base font-bold leading-none ${s.cls}" aria-hidden="true">${s.glyph}</span>
              <span class="font-mono text-sm text-zinc-700 dark:text-zinc-200 truncate">${esc(a.group)}</span>
            </span>
            <span class="flex items-center gap-3 shrink-0 text-xs tnum" title="${esc(describe)}">
              <span class="text-zinc-500 dark:text-zinc-400">${a.failed}/${a.total}${a.skipped ? ` · ${a.skipped} skip` : ""}</span>
              <span class="w-14 text-right font-mono font-medium text-zinc-700 dark:text-zinc-200">${rate}</span>
              <span class="w-14 text-right font-mono text-zinc-500 dark:text-zinc-400">${cover}</span>
              <span class="sr-only">${esc(describe)}</span>
            </span>
          </li>`;
        })
        .join("");
      // Two adjacent percentages need naming: the same pair of figures read
      // either way round without a label, and they are not interchangeable.
      // Right-anchored to the same fixed widths as the rows, so the heads sit
      // over their own columns.
      return `
      <section>
        <h3 class="text-xs uppercase tracking-wide font-semibold text-zinc-500 dark:text-zinc-400 mb-1">${esc(TIER_LABEL[t] || t)}</h3>
        <div class="flex items-center justify-end gap-3 pb-1 text-[0.6rem] uppercase tracking-wide text-zinc-400 dark:text-zinc-500" aria-hidden="true">
          <span class="w-14 text-right">diverges</span>
          <span class="w-14 text-right">covered</span>
        </div>
        <ul class="divide-y divide-zinc-100 dark:divide-white/5">${rows}</ul>
      </section>`;
    })
    .join("");
}
