---
title: Taxonomy & knowledge-base schema
type: meta
category: Meta
subcategory: Schema
status: stable
tags: [gokafka, meta, taxonomy]
updated: 2026-06-30
---

# Taxonomy & knowledge-base schema

How every note in this vault is categorized so the graph, Bases, and the graph/visualization plugins stay coherent and queryable. New notes MUST follow this schema.

## Frontmatter (every note)
```yaml
---
title: Human title
type: <node type>            # see below
category: <Top category>     # see below
subcategory: <finer bucket>  # see below
status: <lifecycle>          # see below
tags: [gokafka, <type>, <topic…>]
updated: YYYY-MM-DD
# optional: related, url, license
---
```

- **`type`** — node kind, drives node color in Extended/3D Graph: `architecture`, `protocol`, `package`, `feature`, `compatibility`, `competitor`, `concept`, `decision`, `source`, `audit`, `research`, `history`, `meta`, `moc`, `dashboard`, `hot`, `log`.
- **`category`** — top-level domain, drives Bases grouping & graph folders: `Architecture`, `Protocol`, `Packages`, `Features`, `Compatibility`, `Competitors`, `Concepts`, `Decisions`, `Research`, `History`, `Meta`, `Home`.
- **`subcategory`** — finer bucket within the category (e.g. Packages→Producer/Consumer/Admin/Schema/Testing/Observability/Transactions; Features→Groups/EOS/Security/Observability; Concepts→KIP-848/KIP-932).
- **`status`** — lifecycle: `ga`, `stable`, `active`, `reference`, `accepted`, `actionable`, `resolved`, `complete`, `draft`, `stale`.
- **`tags`** — always start `gokafka`, then the `type`, then cross-cutting topics (`kip-848`, `kip-932`, `kip-890`, `kip-482`, `eos`, `consumer`, `producer`, `security`, `schema-registry`, `research`, …). Topics span categories; that's the point.

## Category → folder map
| Category | Folder | Holds |
|---|---|---|
| Architecture | `architecture/` | how the client is built (layers, lifecycle) |
| Protocol | `protocol/` | wire protocol, API/KIP coverage, negotiation |
| Packages | `packages/` | one note per Go package / public surface |
| Features | `features/` | user-facing capabilities (EOS, share groups, CSFLE…) |
| Compatibility | `compatibility/` | brokers & versions (Kafka, Redpanda, quirks) |
| Competitors | `competitors/` | the four alternative clients + parity matrix |
| Concepts | `concepts/` | protocol concepts (KIP-848 / KIP-932 internals) |
| Decisions | `decisions/` | ADRs |
| Research | `questions/`, `sources/` | research syntheses, audits, external sources |
| History | `history/`, `log.md` | releases, activity log |
| Meta | `meta/` | bases, templates, this schema — **excluded from dashboards** |
| Home | root | [[index]], [[Dashboard]], [[hot]], [[repo-docs]] |

## Graph & visualization plugins
This vault is tuned for five plugins (all installed under `.obsidian/plugins/`):
- **3D Graph** — a 3D force graph of the link structure. Fed by clean, path-form wikilinks + a `## Related` section on every note.
- **Extended Graph** — colors/filters nodes by `type`/`category` **property** and by **tags**, plus folder grouping. Configure colour groups in its settings; the schema above gives it consistent keys.
- **Charts View** — `chartsview` code blocks (Column/Pie/Radar/Line…) on the [[Dashboard]] render the analytics.
- **Contribution Graph** — `contributionGraph` code blocks render release/activity heatmaps.
- **Smart Connections** — semantic (embedding) backlinks; its `.smart-env/` index is git-ignored and excluded from token reads.

## Conventions
- Every note ends with a **`## Related`** section of 4–8 topical wikilinks (path form, no `.md`).
- Authoritative facts come from the repo (`README.md`, `CHANGELOG.md`, `docs/CONFORMANCE.md`); the wiki synthesizes + connects them — see [[repo-docs]].
- Retrieval order for answering questions: [[hot]] → [[Dashboard]]/[[index]] → the one specific note. Read the ONE note, never whole folders.

## Related
- [[index]] · [[Dashboard]] · [[meta/conventions]] · [[repo-docs]]
