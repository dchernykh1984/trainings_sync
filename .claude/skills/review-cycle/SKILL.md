---
name: review-cycle
description: Review a branch or PR for correctness and cleanup, then land the fixes. Use when asked to review the current diff or a PR before it merges.
---

# Review cycle

- Review the actual diff against `origin/main`, not the whole tree.
- Prefer real correctness bugs; report cleanup only when it clearly earns its place. Do
  not invent findings to hit a count.
- Apply the valid fixes on the branch, re-run the local gate (`uv run pytest`,
  `uv run ruff check .`, `uv run mypy app tests`), and push.
- Keep each fix a one-line Conventional Commit with no attribution or co-author line.
- Then drive CI back to green (see the shipping-a-change skill) before handing back.
