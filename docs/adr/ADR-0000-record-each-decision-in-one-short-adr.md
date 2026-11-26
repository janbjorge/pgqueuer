# ADR-0000: Record each design decision in one short ADR

## Status

Proposed

## Summary

For design decisions in PgQueuer, we keep one short record in the repository
for each decision. Decisions in PRs, chats and trackers become lost. We do not
use trackers or long design documents for decisions. Each decision then has
one place. The cost is one documentation PR for each decision.

## Context

We make decisions in PR threads, reviews, agent chats and trackers. These are
not records. Because of this, the same questions occur again.

## Decision

Write a record when a real alternative exists and a change of the decision has
a cost. Use the sections of this record, in the same order.

Put one decision in each record, and keep the record to one page. Write the
title as a sentence that states the decision. The summary gives the context,
the problem, the decision, the rejected option, the benefit and the cost. Cite
code, mechanisms and values by path, and do not explain them. Give at least
one real alternative and at least one cost. The sections "Not covered" and
"Revisit when" show the limits of the decision. Write each record in
ASD-STE100.

A record starts as `Proposed`. It then becomes `Accepted` or `Rejected`.
Use `Accepted (retroactive)` for behaviour that exists before its record. Do
not change an accepted record. Write a new record, and set the status of the
old record to `Superseded by ADR-NNNN`.

Name the file `docs/adr/ADR-NNNN-<slug>.md`, with the next free number. Do not
use a number again. Add each record to the index in README.md. Put one record
in each PR, with no code in it. Keep rejected proposals. A record refers only
to merged records.

Trackers, code comments and AGENTS.md refer to records by number. They do not
hold decisions.

## Consequences

Each decision has one place, and short records are easy to read and to
replace. But each decision requires an extra PR. Also, a small correction to
an accepted decision requires a new record.

## Alternatives considered

We can keep decisions in trackers and PR descriptions. This adds no step. We
did not choose it, because we delete trackers and we do not search PR threads.

The full MADR template is more complete. We did not choose it, because each
extra section makes a long design document more probable.

## Not covered by this ADR

This record does not control the approval of records, publication or tools.
Today, the maintainer approves records.

## Revisit when

Examine this record again when more maintainers join, or when new records are
often longer than one page.

## References

This record uses ideas from Michael Nygard (2011), Uwe Zdun et al. (2013),
MADR 4.0, Olaf Zimmermann (2023) and Martin Fowler.
