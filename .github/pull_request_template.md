## Summary
- What changed:
- Why:

## Validation
- [ ] `make doctor`
- [ ] `make check`
- [ ] `make gate-tests` (required for workflow/config changes)
- [ ] `make quality`
- [ ] `quality/status.json` says `passed`, fingerprints match, and no gate was skipped

Include exact rerun commands or focused log excerpts for any unusual failure investigation.

## Coverage
- Aggregate function, region, and line floors remain 95%.
- Changed executable lines meet 95% and the small-patch floor.

## Risk and Rollback
- Risk:
- Rollback plan:
