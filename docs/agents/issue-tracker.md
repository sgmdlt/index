# Issue tracker: beads репозитория courts

У index нет своего трекера. Задачи живут в beads репозитория courts: `bd` запускается из
`/home/ltdmgs/courts` (`cd /home/ltdmgs/courts && bd ...`). ID — `courts-xxxx`; коммиты
index ссылаются на них в теме `(courts-xxxx)`. GitHub Issues на `sgmdlt/index` не используются.

Все задачи index помечаются меткой **`repo:index`**. Сквозные эпики, которые затрагивают
и courts, и index (например, формула id дела), — норма: у них та же метка плюс
дочерние задачи по каждому репозиторию.

## Conventions

- **Create**: `bd create "Title" -t <bug|feature|task|epic|chore|decision> -p <0-4> -l repo:index --body-file - <<'EOF' ... EOF`. Add `--parent <epic-id>`, `--deps discovered-from:<id>` as needed.
- **Read**: `bd show <id>` (+ `bd comments <id>`; `--json` on either for structured output).
- **List**: `bd list --status open -l repo:index`, `bd ready -l repo:index`, filter further with `--parent <epic-id>`.
- **Sub-issue**: `bd create ... -l repo:index --parent <parent-id>`.
- **Blocking**: `bd dep add <blocked-id> <blocker-id>`.
- **Comment**: `bd comments add <id> "..."` (or `-f file`).
- **Labels**: `bd label add <id> <label>` / `bd label remove <id> <label>`.
- **Claim**: `bd update <id> --claim`.
- **Close**: `bd close <id> --reason "..."`.
- **Side quest found mid-task**: `bd create -t bug -l repo:index ... --deps discovered-from:<current-id>`.

## Specs

A spec is an epic: its description holds the full spec (`/to-spec` template), labelled
`ready-for-agent` and `repo:index`; tickets are its children (`--parent`). It closes with the epic.
Durable decisions are extracted into `docs/adr/`, terms into `GLOSSARY.md` — a closed
epic is not where an agent looks for how the system works today.

No spec or plan files: a spec is the epic (`/to-spec`), a plan is its child tickets
(`/to-tickets`). Historical specs are in their epics' `design` field, historical
plans in their epics' comments (`bd comments <id>`).

## When a skill says "publish to the issue tracker"

`cd /home/ltdmgs/courts && bd create ... -l repo:index` (an epic for a spec, tasks with `--parent` for its tickets).

## When a skill says "fetch the relevant ticket"

`bd show <id>` + `bd comments <id>` (from `/home/ltdmgs/courts`).

## Wayfinding operations

Used by `/wayfinder`. The **map** is an epic; **children** are its sub-issues. All carry `repo:index`.

- **Map**: `bd create -t epic -l wayfinder:map,repo:index "..."`; Notes / Decisions-so-far / Fog live in the description (`bd update <id> --description ...`).
- **Child ticket**: `bd create --parent <map-id> -l repo:index,wayfinder:<research|prototype|grilling|task> "..."`, question in the body.
- **Blocking**: `bd dep add <child> <blocker>`; unblocked when every blocker is closed.
- **Frontier**: `bd ready --parent <map-id> --unassigned`; first in list wins.
- **Claim**: `bd update <id> --claim` — the session's first write.
- **Resolve**: `bd comments add <id> "<answer>"`, `bd close <id>`, then append gist + `<id>` to the map's Decisions-so-far.
