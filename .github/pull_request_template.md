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
- Aggregate floors remain 99.5% functions / 99.0% regions / 99.4% lines.
- Changed executable lines meet 95% and the small-patch floor.

## Risk and Rollback
- Risk:
- Rollback plan:
