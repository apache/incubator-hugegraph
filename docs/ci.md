# CI policy

[HugeGraph-Server CI](../.github/workflows/server-ci.yml) is the single entry point for
PRs, supported branch pushes, weekly coverage and manual validation. Its preflight job
selects affected tests, reads the project Java version, checks CI policy and verifies
license headers and RAT. Selected suites run through reusable workflows in the same run.

CI results become advisory after [`.asf.yaml`](../.asf.yaml) is applied. It requests no required CI contexts
and retains one approving review. This protection change takes effect after the configuration
is merged and synchronized by Gitbox; verify live protection then. Failed tests keep their
real failed results. Valid selection and Java outputs allow selected tests to run even when
the preflight license checks fail.

For migration, preflight keeps the existing `check-license` check name on the same runner.
A temporary `Server memory tests` check verifies the selected Memory result while live protection
still requires that name. It uses one lightweight `ubuntu-slim` runner and repeats no tests.
After Gitbox applies the new configuration, this bridge skips without allocating a runner;
no Infra intervention or change to review protection is needed.

## What runs

| Validation | Ordinary PR | Push to master | Weekly full run |
| --- | --- | --- | --- |
| Memory and RocksDB | Full unit, core and API tests when affected | No repeated full tests | Full tests |
| macOS ARM | Startup, JNI, graph read/write and restart smoke when affected; full core/API for platform inputs | Affected smoke | Full core/API |
| HBase and RISC-V | Directly related backend/platform or validation inputs only | No repeated tests | Full validation |
| PD, Store, HStore and Cluster | Affected modules and known consumers | Current-commit distributed smoke | Full group |
| CodeQL | Authentication, authorization, security, POM or CodeQL configuration inputs | No repeated scan | Full scan |
| Docker, Helm and dependency audit | Corresponding inputs | Corresponding deployment smoke or checks | Full validation |

macOS uses one `macos-15` ARM job for either smoke or full validation. Intel macOS is removed.
The weekly run uses `cron: '0 6 * * 1'` with `timezone: Asia/Singapore`: Monday at 06:00,
on the latest default-branch commit. GitHub may delay scheduled runs during high load;
see [schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

### Affected inputs

The rules live in [ci-policy.py](../.github/scripts/ci-policy.py). Ordinary Server changes
select both Memory and RocksDB, plus ARM smoke. They do not select the complete PD/Store group.
PD and Store changes select their own tests and downstream consumers; Commons and Struct
select their known shared consumers. HStore and Cluster additionally follow distributed
backend contracts, protocols, metadata, graph lifecycle and scheduling inputs. Distribution,
startup and RocksDB inputs select ARM full validation and their deployment consumers.

Unclassified paths or an unavailable diff/API conservatively select mainline coverage.
HBase and RISC-V remain limited to explicit related inputs, weekly coverage or manual selection.
The classifier uses paths, not PR titles or semantic interpretation of changed text.
Contract scripts explicitly select their executing suites without adding runtime consumers.

### Documentation

Documentation-only ordinary PRs and pushes allocate just the preflight CI runner. This count
excludes the non-test [Auto PR Commenter](../.github/workflows/auto-pr-review.yml), which runs
once when a PR opens. Scheduled and manual selections still run their requested suites.

Plain documentation includes regular-file root prose, README/AGENTS files and supported prose
or static images under `docs/`, including nested module documentation. Runtime resources,
configuration, source, fixtures, templates and executable or symbolic-link files do not qualify;
for example, a test fixture ending in `.txt` still selects its module's tests.

## Manual validation and reruns

Use **Actions → HugeGraph-Server CI → Run workflow**, select a branch and choose `test-set`:

| Test set | Validation |
| --- | --- |
| `mainline` (default) | Full mainline modules, deployment/dependency checks and CodeQL; ARM smoke |
| `distributed` | Struct, PD, Store, HStore and Cluster, plus distributed smoke |
| `macos-arm` | ARM smoke and full RocksDB core/API tests |
| `hbase`, `riscv64`, `codeql` | The corresponding complete specialist validation |
| `full` | All retained suites, identical to weekly coverage |

Fork contributors run additional validation in their fork's Actions. The manual entry point
does not accept a PR number, apply labels or reuse another workflow's results.
Automatic retries are disabled. Use GitHub's native rerun controls for failed jobs or a run;
a rerun retains its original commit and event, subject to the preflight freshness checks.

## Java runtime compatibility

A positive suite passes only when its tests succeed and its current-run output has no
Java runtime compatibility errors. Server, PD/Store, Commons, Struct and Cluster test
steps capture both stdout and stderr before running `check-java-runtime-logs.sh`.
Startup and Console smoke tests also inspect their output before cleanup; Cluster nodes
archive logs before deletion through the test-only `hugegraph.ct.logArchive` property.
Missing or unreadable required logs fail the check.

The gate rejects denied module access and unknown or removed JVM options, including
exceptions caught and printed by dependencies. Ordinary business errors and deliberate
negative fixtures are not compatibility failures. Native smoke additionally uses `--native`
for its existing native linkage checks.

Launchers and tests share each component's `jvm-module.options`: PD and Store open
`java.lang` for Hessian, while Console and dump-store reuse Server's existing options.
Required module options are independent of custom `JAVA_OPTIONS` or `-j` flags; Surefire
retains the JaCoCo agent. CI continues to use the project Java runtime and its existing
suite-selection and required-check policy.


## Freshness, runner use and budgets

PR selection uses the cumulative diff against the checked-out merge commit's base. A source
PR followed by a documentation-only commit still tests the cumulative source changes.
Preflight verifies that the event merge commit's second parent matches the current PR head.
A stale PR head fails preflight. Target-branch advancement alone does not invalidate a run.
Plans and selection summaries describe this run; previous results are never reused.

A new PR head cancels the previous PR run, including its called workflows. Manual and weekly
runs remain independent. Each job reports its own result; there are no separate post-test
aggregation runners after the protection migration or end-of-run metadata checks.

Startup contracts run inside RocksDB validation, and Store stop contracts run inside Struct.
Docker's static checks, deployment contracts and current-image validation share one runner.
The Docker job has a 90-minute total budget, with 25 minutes for deployment contracts and
45 minutes for image build/runtime validation. Distributed smoke builds current PD, Store
and Server images and checks authentication, readiness and graph read/write. It does not
substitute published images for the current commit.

PD, Store and Commons have total job budgets of 30, 45 and 20 minutes. Cluster has 120 minutes
for its two 45-minute test windows and build/diagnostics. Other budgets are in the workflows.
TinkerPop suites keep their dedicated branch policy: the source, target or push/manual branch
must start with `release-`, `test-` or `tinkerpop-`. Executed suites require non-empty reports
with actual tests.

Each separate job or matrix item requests a runner; steps within a job share one.
[Standard hosted runners are free for public repositories](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories),
but [plan-dependent total and macOS concurrency limits](https://docs.github.com/en/actions/reference/limits#job-concurrency-limits-for-github-hosted-runners)
still apply. Reducing job allocations and long executions lowers shared-pool demand;
it does not guarantee a specific queue delay.
