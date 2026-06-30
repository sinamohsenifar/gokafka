---
title: Vault conventions
type: meta
category: Meta
subcategory: Conventions
status: stable
tags: [gokafka, meta]
updated: 2026-06-30
---

# Vault conventions

How this wiki is organized, so new notes stay consistent and the [[Dashboard]] Bases keep working. The full category/sub-category schema lives in **[[meta/taxonomy|the taxonomy]]**; this note is the quick reference.

## Frontmatter (properties)
Every note has:
```yaml
---
title: Human title
type: <one of below>
tags: [gokafka, ...]
updated: YYYY-MM-DD
---
```
`type` drives the Bases dashboards. Values in use:
`moc`, `dashboard`, `meta`, `log`, `architecture`, `package`, `protocol`, `feature`, `compatibility`, `decision`, `competitor`, `history`.

Extra properties by type:
- **decision** (ADR): `status` (proposed / accepted / superseded).
- **competitor**: `url`, `license`.

## Folders
| Folder | Holds |
|---|---|
| `architecture/` | how the client is built (layers, lifecycle) |
| `packages/` | one note per Go package / public surface |
| `protocol/` | wire protocol, API/KIP coverage, negotiation |
| `features/` | user-facing capabilities (EOS, share groups, CSFLE…) |
| `compatibility/` | brokers & versions (Kafka, Redpanda, quirks) |
| `decisions/` | ADRs |
| `competitors/` | the four alternative clients + parity matrix |
| `history/` | releases, session logs |
| `meta/` | bases, templates, this file — **excluded from the dashboard** |

## Linking
- Use `[[path/note|Label]]` wikilinks (relative, no `.md`). Link liberally — backlinks and graph view do the rest.
- Every note ends with a **Related** section.

## Tooling
- **Bases** (`meta/*.base`) — dynamic tables; embed with `![[name.base]]`.
- **Templates** (`meta/templates/`) — [[meta/templates/page|page]], [[meta/templates/adr|adr]]. New note → Templates: Insert.
- **Canvas** (`meta/architecture-map.canvas`) — visual flow map.

## Source of truth
The repo's `README.md`, `CHANGELOG.md`, and `docs/CONFORMANCE.md` are authoritative; wiki pages synthesize and cross-link them.

## Related
- [[meta/taxonomy|Taxonomy & schema]] · [[index]] · [[Dashboard]] · [[repo-docs]]
