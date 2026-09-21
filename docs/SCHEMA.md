# Schema reference

Two schemas matter: the **raw zone** the generator produces (the pipeline's input contract) and the
**report object** the dispatcher reads and the agent downloads (the pipeline's output contract).
Both are asserted in the test suite, so they cannot drift from the code.

## Raw zone (input)

Key layout — built and parsed by `agent_reports.common.keys`:

```
raw/dt=YYYY-MM-DD/source=agents/part-00000.csv
raw/dt=YYYY-MM-DD/source=policies/part-00000.csv
raw/dt=YYYY-MM-DD/source=claims/part-00000.csv
raw/dt=YYYY-MM-DD/_dataset_manifest.json        # row counts + generator config
```

Partitioning is `row_index % partitions`, so re-sharding changes which file a row lands in but never
the rows themselves (`tests/unit/test_generator.py::test_sharding_does_not_change_the_rows`).

### `agents` (the day's roster, one row per agent)

| Column | Type | Notes |
| --- | --- | --- |
| `agent_id` | string | `AGT-\d{6}` — primary key, also the report partition value |
| `agent_name` | string | synthetic display name (small fixed name lists) |
| `email` | string | `agt-000123@example.com` — reserved, non-routable domain |
| `region` | string | North, South, East, West, Central |
| `branch` | string | city-level office within the region |
| `manager_id` | string | another `AGT-` id (or the agent itself for the first row) |
| `joined_on` | date | ISO date, 30–2200 days before the report date |

### `policies` (written premium)

| Column | Type | Notes |
| --- | --- | --- |
| `policy_id` | string | `POL-\d{10}` — primary key |
| `agent_id` | string | FK → `agents.agent_id` |
| `customer_id` | string | opaque synthetic key `CUS-\d{9}` |
| `product` | string | Individual Health, Group Health, Personal Accident, Term Life |
| `policy_start` / `policy_end` | date | ISO dates, 12-month term |
| `sum_insured` | decimal(18,2) | cover in INR, a round number of lakhs |
| `premium` | decimal(18,2) | `sum_insured × a product rate drawn uniformly from the product band` |
| `commission_rate` | decimal(9,4) | contracted rate, 4dp: 0.0625 (Group Health) … 0.1825 (Term Life) |
| `status` | string | Active, Renewed, Lapsed (70/20/10) |

### `claims` (zero or more per policy)

| Column | Type | Notes |
| --- | --- | --- |
| `claim_id` | string | `CLM-\d{10}` — primary key |
| `policy_id` / `agent_id` | string | FKs (agent denormalised so aggregation needs no join) |
| `claim_date` | date | within the policy period, never after the report date |
| `claim_type` | string | Cashless, Reimbursement |
| `diagnosis_chapter` | string | ICD-10 chapter letter only (coarse, non-identifying) |
| `claimed_amount` | decimal(18,2) | lognormal severity × sum insured, capped at 100% of cover |
| `settled_amount` | decimal(18,2) | 0 while Pending / when Rejected, else 80–100% of claimed |
| `status` | string | Settled 55%, Approved 20%, Rejected 15%, Pending 10% |
| `tat_days` | integer | 1–45 |
| `hospital_tier` | string | Tier-1, Tier-2, Tier-3 |

**No PII, no real data.** Every value is generated from a seeded `random.Random`; customer ids are
opaque, emails use the reserved `example.com` domain, and diagnosis is a chapter code rather than
free text. The generator is deterministic: the same `DatasetConfig` produces byte-identical files
(`tests/unit/test_generator.py::test_determinism_is_byte_identical`).

Claim incidence is a config knob (`claims_per_policy`, default 0.6) that scales the *relative* risk
between products, so `DatasetConfig.for_total_rows(50_000)` reliably lands above 50,000 rows
(measured: 52,471).

## Report object (output)

One object per agent per day:

```
reports/dt=YYYY-MM-DD/agent_id=AGT-000123/report.csv
```

Written by both the chunker Lambda and the PySpark job, byte for byte identically **for
generator-shaped input** — the four divergences that used to break that claim, and the preconditions
that remain, are listed in
[VERIFY.md](VERIFY.md#byte-identity-what-it-means-and-its-preconditions). Columns
(`agent_reports.common.report.REPORT_COLUMNS`):

| # | Column | DETAIL rows | TOTAL row |
| --- | --- | --- | --- |
| 1 | `row_type` | `DETAIL` | `TOTAL` |
| 2 | `agent_id` | the agent | the agent |
| 3 | `agent_name` | from the roster | empty |
| 4 | `region` | from the roster | empty |
| 5 | `branch` | from the roster | empty |
| 6 | `policy_id` | `POL-…` | empty |
| 7 | `product` | product line | empty |
| 8 | `policy_start` | ISO date | empty |
| 9 | `policy_end` | ISO date | empty |
| 10 | `sum_insured` | 2dp | empty |
| 11 | `premium` | 2dp | sum |
| 12 | `commission` | `premium × commission_rate`, 2dp, HALF_UP | sum |
| 13 | `policy_count` | `1` | number of policies |
| 14 | `claim_count` | claims against the policy | sum |
| 15 | `claim_amount` | 2dp | sum |
| 16 | `settled_amount` | 2dp | sum |
| 17 | `loss_ratio` | `claim_amount / premium`, 4dp, `0.0000` when premium is 0 | same formula on the totals |

Formatting rules that the tests pin down:

- money is rendered with exactly two decimals, `ROUND_HALF_UP` (not the Decimal context default
  `ROUND_HALF_EVEN` — a half-paise tie must round up, and the Spark job's `round()` is HALF_UP);
- `loss_ratio` has exactly four decimals;
- rows are ordered: `DETAIL` rows sorted by `policy_id`, then exactly one `TOTAL` row;
- LF line endings only, no BOM, no quoting unless a value contains a comma or quote;
- the header is always exactly the 17 columns above, in that order.

The dispatcher reads this object back (`agent_reports.common.report.parse_report_totals`) to
personalise the email with real numbers, and refuses to send a link to an object that does not exist.

## State zone (internal)

```
state/runs/dt=YYYY-MM-DD/manifest.json                   # orchestrator fan-out manifest
state/dispatch/dt=YYYY-MM-DD/agent_id=AGT-000123.json    # idempotency + delivery state
state/quarantine/dt=YYYY-MM-DD/message-<sha256-16>.json  # messages we refuse to process
```

The dispatch marker is a small state machine (`dispatching` → `sent` / `failed`) with a lease so a
crashed invocation cannot wedge an agent, and a permanent failure cannot be retried forever. See
`agent_reports/common/idempotency.py` for the transition table and `docs/RUNBOOK.md` §5a for the
operator view of it.
