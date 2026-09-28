<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: Copyright 2026 AVEVA
-->

# Research inputs for the secondary-cache redesign

Raw background-research outputs that fed
[`../secondary-cache-performance.md`](../secondary-cache-performance.md). They are committed so
that later revisions of the proposal can be diffed against the evidence they were derived from,
rather than against an undocumented memory of it.

| File | Provenance | Fidelity |
|---|---|---|
| [`prior-art-survey.md`](prior-art-survey.md) | Background research agent, 2026-09-28 | **Verbatim.** Unedited agent output. |
| [`rocksdb-secondary-cache-contract.md`](rocksdb-secondary-cache-contract.md) | Background exploration agent over the local RocksDB 11.12.0 tree, 2026-09-28 | **Reconstructed.** The raw transcript was lost to temp-file cleanup before it could be committed; this is the distilled set of findings that were actually incorporated into §4 of the proposal. Citations are reproduced as recorded, but line numbers are approximate and should be re-verified before being relied on. |

## Caveats

- These are **research notes, not specifications**. Where they disagree with
  `../secondary-cache-performance.md`, the proposal wins — it is the document that was reviewed.
- Confidence is uneven and is flagged inline. `prior-art-survey.md` ends with an explicit
  "Notable Gaps" section listing what could not be verified; read it before quoting any number.
- Figures quoted from papers and from third-party source trees were correct as of the commit refs
  named inline. Nothing here is re-verified on a schedule.

## Versioning

Initial version corresponds to proposal revision `v0.1` (commit `f313625`).
