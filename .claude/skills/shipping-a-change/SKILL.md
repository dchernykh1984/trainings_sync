---
name: shipping-a-change
description: Branch, commit, open a pull request, and drive CI to green in this repo. Use whenever you are about to commit, push, open a PR, or check CI status.
---

# Shipping a change

## Branch

- Never commit to `main`. `git fetch origin && git switch -c <type>/<slug> origin/main`.
- The working tree may hold the maintainer's local data or untracked files. Stage only
  files you changed (`git add <path>`), never `git add -A`.

## Commit

- One line, Conventional Commits: `git commit -m "type(scope): summary"`. No body and no
  `Co-Authored-By` trailer. `cz check --rev-range origin/main..HEAD` runs in CI, and
  release-please builds CHANGELOG from these subjects, so the type matters (`feat`/`fix`
  are released; `chore`/`docs`/`test`/`style`/`refactor` are not).

## Before pushing

- `uv run pytest` (coverage gate 90%), `uv run ruff check .`,
  `uv run ruff format --check .`, `uv run mypy app tests`. pre-commit runs the same set
  on commit.
- Every tracked file must be ASCII (the `no-non-ascii` hook covers markdown too). `uv run`
  may rewrite `uv.lock`; keep it out of feature commits with `git checkout uv.lock` unless
  the lock itself is the change.

## Pull request

- `git push -u origin <branch>` then `gh pr create --base main --title "..." --body "..."`.
- PR body is real content only: no "Generated with Claude Code" line and no co-author
  footer.

## Watch CI to green

- Poll the authoritative rollup, not `gh pr checks` (its per-check status lags and can
  show `pending` after a job has finished):

  ```
  gh pr view <n> --json statusCheckRollup \
    --jq '[.statusCheckRollup[] | {name:(.name//.context), s:(.conclusion//.state)}]'
  ```

- Every check must be SUCCESS before requesting review.
