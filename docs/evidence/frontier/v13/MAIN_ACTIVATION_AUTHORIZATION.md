# Main authorization — CONSUMED; retry failed and compute is OFF

The one authorized attempt ran under PID67945 and ended with verified cleanup at 2026-09-17T05:35:46.776Z. Baseline acct101 failed with 1,000 failed-or-ambiguous rows; acct102/restart did not run. This document authorizes NO further activation or retry. See PROGRESS.md and actual receipts. Source and controls remain frozen; only offline read-only transport diagnosis is active.

## Original one-attempt authorization

Independent original review 5215f03480e54100812a7a4ad88503f2 found a reference-gate blocker. Main preserved all75inputs and fixed exactly the reference-role list; focused review 6a17099bd77e453ca16301f789707ed2 independently replayed old failure/new success and approved the correction. Actual report: ../s13-monitor-review/REVIEW-CORRECTION.md. Reviewed freeze cac74043b4f9c36e3a0f1d51268856502c2fd5d38c3a4330be856460f9f556b4 /43controls/78files/208boundcases. Source58f405 and driver23e/profile215f remain unchanged.

Under the user's continuing Railway stress-test authorization, Main now authorizes exactly one cold configuration, Varve-only reuse of pin a88e2c35-596f-41d4-b383-06857bf43108, and ONE run-campaign.mjs supervisor. The controller attests source/binary/freshDB before starting pinned reference images and acct101/acct102, then verifies restart and owned-only cleanup. Original service/volumes/caps remain protected. No rebuild, retuning, extra timed retry, fixture regeneration or source change is authorized. Any ambiguous activation stops for reconciliation. Earlier author/reviewer offline-only scope is not retroactively widened.

Only Main holds activation/controller authority. No other live operator or controller delegated. Historical artifacts and old controller remain untouched. No performance result is asserted by this authorization.
