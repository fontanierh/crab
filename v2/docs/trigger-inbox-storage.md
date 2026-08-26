# Trigger inbox storage

`trigger-inbox` stores its queue in SQLite. Every enqueue, claim, lease extension and settlement is
one immediate transaction. File-backed stores enable WAL mode with full synchronous durability.

## Schema lifecycle

- `PRAGMA user_version = 1` is the initial Crab v2 trigger schema.
- Opening a new database performs the idempotent `v0 -> v1` migration transaction.
- Opening a database with a newer or unknown schema fails closed with `StorageUnavailable`.
- Future non-additive changes require an explicit version step and restart/idempotency tests.

## Queue invariants

- `(source_id, deduplication_key)` identifies one immutable logical trigger.
- Identical retries return the original record; conflicting retries fail.
- Claims lease a contiguous ready prefix of one lane in insertion order.
- An active lease or delayed head record blocks later records in that lane.
- Expired leases become pending and may be claimed again with a new token and incremented attempt.
- Settlement requires the current unexpired lease token.
