---
name: garmin-strava-sync
description: How trainings_sync moves activities between Garmin Connect, a local folder, and Strava. Use when working on the sync engine, activity file formats, or rate limiting. Contains no secrets.
---

# Sync engine

- Direction: Garmin Connect <-> local folder (FIT/GPX/TCX), plus optional upload to Strava.
- Credentials and tokens for Garmin and Strava are provided at runtime and are SECRETS:
  never hardcode, log, or commit them. In tests never call the real services - patch the
  clients and assert on the requests and on how responses are handled.
- Strava rate limits are a real, recurring failure mode (the README warns about it).
  Handle 429 / rate-limit responses gracefully and log them; do not silently stall. Cover
  the rate-limit and backoff/retry paths with tests.
- Keep parsing and matching of activity files (FIT/GPX/TCX) as pure functions covered by
  fixture files, separate from the CLI and GUI shells, so they stay unit-testable.
