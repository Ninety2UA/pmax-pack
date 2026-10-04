#!/usr/bin/env bash
# Retained contracts: plan output, refusals, phase-25 pointer, phase-80 parity, and CI.
set -euo pipefail

# shellcheck source=lib.sh
# shellcheck disable=SC1091
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# Public migration and private operating duties are one ordered contract.
uv run python - "$ROOT" <<'PY_U17'
from pathlib import Path
import re
import sys
import unittest

root = Path(sys.argv[1])
PIN_EVENTS = []
# Public hex-placeholder allow-list is empty. <digest>, <commit>, PROJECT,
# and RECOVERY are textual placeholders and never need a hex exemption.
PUBLIC_HEX_PLACEHOLDERS = frozenset()

REVIEWED_GO_NO_GO_SOURCES = {'Step 0 operator role / UTC time / rollback pair': 'Current Job digest beside recorded anchor '
                                                    'digest; image-only readback below, no '
                                                    'principal addresses',
 'CLI and ADC same-operator check before each pass': 'Identity recipe below: match/mismatch only, '
                                                     'user ADC, impersonation unset',
 'Paused state and settled executions': '`gcloud scheduler jobs describe pmax-pack-daily '
                                        '--project="$PROJECT" --location="$REGION" '
                                        "--format='value(state)'`; `gcloud run jobs executions "
                                        'list --job=pmax-pack-daily --project="$PROJECT" '
                                        '--region="$REGION" '
                                        "--format='json(metadata.name,status.completionTime)'` "
                                        'with every completionTime present; phase-68 lease guard',
 'Accepted storage / reporting window / ALTER_DAY / published AS_OF': 'Validated config and '
                                                                      'explicit storage ruling; '
                                                                      'ALTER_DAY for accepted '
                                                                      'loss, AS_OF from the '
                                                                      'published report',
 'Config saved / original generation / checksum': 'Config save commands below; '
                                                  '`config-pre-v2.1.0.generation.txt`',
 'Uploaded config generation / phase-65 match': '`gcloud storage objects describe "$CONFIG_URI" '
                                                '--project="$PROJECT" '
                                                "--format='value(generation)'`; `deployment.yaml`",
 'editor count and config generation / listed organization count': 'Count-only config recipe '
                                                                   "below; verify every editor's "
                                                                   'organization privately, never '
                                                                   'paste addresses',
 'Raw loss planning value / pass-2 accepted value: click days / rows / bytes / boundary-day rows': 'Repeat '
                                                                                                   'migration '
                                                                                                   'Q1 '
                                                                                                   'on '
                                                                                                   'pass-2 '
                                                                                                   'ALTER_DAY '
                                                                                                   'before '
                                                                                                   'phase '
                                                                                                   '89, '
                                                                                                   'joined '
                                                                                                   'to '
                                                                                                   'the '
                                                                                                   'derived '
                                                                                                   'ALTER '
                                                                                                   'list; '
                                                                                                   're-accept '
                                                                                                   'after '
                                                                                                   'any '
                                                                                                   'day '
                                                                                                   'change',
 'Mart loss planning value / pass-2 accepted value: click days / rows / bytes / boundary-day rows': 'Repeat '
                                                                                                    'migration '
                                                                                                    'Q1 '
                                                                                                    'on '
                                                                                                    'pass-2 '
                                                                                                    'ALTER_DAY '
                                                                                                    'before '
                                                                                                    'phase '
                                                                                                    '89; '
                                                                                                    'keep '
                                                                                                    'both '
                                                                                                    'planning '
                                                                                                    'and '
                                                                                                    'accepted '
                                                                                                    'values',
 'Raw and marts max_time_travel_hours / planned deadline': 'Migration Q2 and metadata recipe '
                                                           'below: recorded hours and '
                                                           'metadata/default provenance for both '
                                                           'datasets; planned ALTER time plus '
                                                           'smaller recorded hours',
 'Every ALTER table: minimum day / row count / additive sums, planning and pass-2 affected range': 'Repeat '
                                                                                                   'migration '
                                                                                                   'Q4 '
                                                                                                   'on '
                                                                                                   'pass-2 '
                                                                                                   'ALTER_DAY '
                                                                                                   'immediately '
                                                                                                   'before '
                                                                                                   'launch; '
                                                                                                   'enumerate '
                                                                                                   'actual '
                                                                                                   'additive '
                                                                                                   'columns '
                                                                                                   'and '
                                                                                                   'bind '
                                                                                                   'accepted '
                                                                                                   'cutoff',
 'Live option map all NULL / live dataset defaults unset': 'Migration Q3 plus Q2 on '
                                                           'raw/marts/ops/reporting; missing row '
                                                           'means NULL only after confirming table '
                                                           'presence',
 'Eight views present / cutover readers = zero / covered log interval': 'VIEWS query below plus Q5 '
                                                                        'over 60 days; exclude '
                                                                        'CREATE_VIEW, DROP_VIEW '
                                                                        'and runtime jobs; record '
                                                                        'actual Data Access '
                                                                        'log-bucket coverage, 30 '
                                                                        'days by default',
 'Observation triples: pass 1 preserved / pass 2 active': 'Separate regular files: immutable `observation-before-<digest>-<generation>-pass1.json`, preserved `observation-before-<digest>-<generation>-pass2.json`, and mutable `observation-before.json`; record preserved pass 1 and active pass 2 in `observation-gate-<digest>.json` beside the after reading',
 'Phase 40 Looker account / agents / editor actAs / iamcredentials enabled': 'IAM readback '
                                                                             'summaries below, '
                                                                             'compared with '
                                                                             'phase-10/40 plan; '
                                                                             'editor and agent '
                                                                             'counts, roles and '
                                                                             'config generation '
                                                                             'only',
 'Conditional operator Token Creator binding count / expiry reuse / stale binding removal': 'Count-only '
                                                                                            'IAM '
                                                                                            'summary '
                                                                                            'and '
                                                                                            'phase-40 '
                                                                                            'evidence; '
                                                                                            'same '
                                                                                            'PMAX_LOOKER_PROBE_EXPIRES_AT '
                                                                                            'on '
                                                                                            'both '
                                                                                            'passes',
 'Phase 75 Looker positive pre-check green': 'Inside phase 75 after its two executions; operator '
                                             'identity through the time-bound grant; a refusal '
                                             'stops phase 75',
 'Optional twin column drop-and-re-add probe': 'Advisory one-column probe on one disposable twin '
                                               'table before pass 2, recorded if run',
 'Labels on datasets / both buckets / Job': 'Phase-80 `resource_labels` in parity evidence: app, '
                                            'env and other_label_count',
 'Phase-70 run / parity run / one signature': 'Bound digest-keyed evidence and operator-authored '
                                              'review; report and parity verified at 85',
 'Phase-88 twin/anchor rehearsal / probes corroborated': '`rehearsal-evidence-<digest>.json`; '
                                                         'latest matching '
                                                         '`looker-probes-<attempt-id>.json` with '
                                                         '`image_ref`, route per dataset, all '
                                                         'audit slots filled; execute its recorded '
                                                         'command with `--freshness`',
 'Phase-89 original_inventory / pass-2 confirmed value / actual deadline': 'First ALTER timestamp '
                                                                           'plus smaller recorded '
                                                                           'raw/marts hours and '
                                                                           'metadata/default '
                                                                           'provenance; compare '
                                                                           'retention-<digest>.json '
                                                                           'and retain the earlier '
                                                                           'verified deadline',
 'Phase-90 failure notification / SKIPPED silence': '`alert-proof-<digest>.json`, `alert-submission-<digest>.json` and `lease-drill-evidence-<digest>-pass1.json` all validated against the active ladder generation, with execution and timestamp provenance matched; operator verified this generation\'s pass-2 email and pass-1 SKIPPED silence before TTY yes or before setting both flags and listing 95 on re-entry',
 'Phase-95 observation triple after / resume': '`observation-after.json`, `observation-gate-<digest>.json` with preserved pass 1, active pass 2 and after readings, signed review, valid alert proof, and `resume-evidence-<digest>.json` bound to the active ladder generation',
 'Immediate reporting reconciliation / eight views absent / six columns present': 'Migration '
                                                                                  'labelled and '
                                                                                  'capped '
                                                                                  'reconciliation '
                                                                                  'SQL on '
                                                                                  'identical full '
                                                                                  'click days and '
                                                                                  'counting/key '
                                                                                  'grain; '
                                                                                  'VIEWS/COLUMNS; '
                                                                                  'phase-70 '
                                                                                  'publish SUCCESS',
 'Immediate options exactly on list / labels present / probes denied': 'Q3 against expected map, '
                                                                       'phase-80 labels, phase-88 '
                                                                       'direct/API/query denial '
                                                                       'evidence and audit '
                                                                       'corroboration',
 'First Looker-side read / principal match / log timestamp and insert ID': 'Data Access summary '
                                                                           'below after a real '
                                                                           'chart refresh: match '
                                                                           'to configured Looker '
                                                                           'account, never the '
                                                                           'address',
 'Immediate flip detector: zero unexplained readers': 'Weekly credential-flip detector procedure '
                                                      'below, covering cutover to now',
 'Scheduled run 1 and 2: run_id / Cloud Run start and end / four-span budget / report lines': 'Saved '
                                                                                              'real '
                                                                                              'reports '
                                                                                              'and '
                                                                                              'execution '
                                                                                              'describes; '
                                                                                              'each '
                                                                                              'under '
                                                                                              '2,000 '
                                                                                              'lines, '
                                                                                              'budget '
                                                                                              'block '
                                                                                              'present, '
                                                                                              'no '
                                                                                              'orphan '
                                                                                              'landing '
                                                                                              'tables',
 'Parent-job reconciliation': 'Run recipe below once per selected nightly baseline; parent_job_id '
                              'IS NULL and exact run_id label; scripts count once, matching '
                              '`(submissions)`',
 '+24 h minimum partition per raw/mart click-day table / drift line': 'Q1 minimum_day compared to '
                                                                      '`AS_OF - '
                                                                      'reporting_window_days - 1` '
                                                                      'using the published report '
                                                                      'date; actual report drift '
                                                                      'silent',
 '+72 h minimum partition per raw/mart click-day table / drift line': 'Same Q1 and actual report; '
                                                                      'record never-expire '
                                                                      'exceptions separately, not '
                                                                      'as failures',
 'Second scheduled run drift / outstanding boundary lag': 'SOFT drift line absent; record any '
                                                          'asynchronously pending row removal '
                                                          'without repeating unchanged ALTERs',
 'Final operator decision / UTC / evidence path': 'All relevant stages completed; no live value '
                                                  'inferred from this template'}

def normalized(text):
    return ' '.join(text.replace('**', '').split())


class MigrationDocumentTests(unittest.TestCase):
    """Pin operator rules where they apply; never print private documents."""

    def document(self, name, private=False):
        self.active_path = name
        path = root / name
        if private and not path.exists() and not (root / 'deployments').is_dir():
            self.skipTest('private document absent from public export')
        self.assertTrue(path.is_file(), f'missing migration document: {name}')
        return path.read_text()

    def scope(self, doc, start, end=None):
        doc, start = doc.replace('**', ''), start.replace('**', '')
        end = end.replace('**', '') if end is not None else None
        self.assertTrue(start in doc, f'missing section: {start}')
        tail = doc[doc.index(start) + len(start):]
        if end is not None:
            self.assertTrue(end in tail, f'missing section end: {end}')
            return tail[:tail.index(end)]
        if start.startswith('#'):
            level = len(start) - len(start.lstrip('#'))
            following = re.search(r'^#{1,' + str(level) + r'} ', tail, re.M)
            if following:
                return tail[:following.start()]
        return tail

    def pin(self, name, text, needle):
        PIN_EVENTS.append((name, self.active_path, 'literal', needle, text))
        with self.subTest(pin=name):
            self.assertTrue(normalized(needle) in normalized(text),
                            f'Document pin {name}: required rule missing or inverted')

    def pattern(self, name, text, expression):
        PIN_EVENTS.append((name, self.active_path, 'regex', expression, text))
        with self.subTest(pin=name):
            self.assertTrue(re.search(expression, text, re.S | re.I) is not None,
                            f'Document pin {name}: required structure missing or inverted')

    def absent(self, name, text, needle):
        PIN_EVENTS.append((name, self.active_path, 'absent', needle, text))
        with self.subTest(pin=name):
            self.assertTrue(needle not in text,
                            f'Document pin {name}: prohibited content present')

    def forbidden_sql(self, name, text, expression):
        PIN_EVENTS.append((name, self.active_path, 'forbidden_sql', expression, text))
        with self.subTest(pin=name):
            self.assertTrue(re.search(expression, text, re.S | re.I) is None,
                            f'Document pin {name}: target reads its own historical version')

    def ordered(self, name, text, items):
        positions = []
        for index, item in enumerate(items):
            self.pin(f'{name}.{index + 1}', text, item)
            positions.append(normalized(text).find(normalized(item)))
        with self.subTest(pin=name):
            self.assertTrue(-1 not in positions and positions == sorted(positions),
                            f'Document pin {name}: required order changed')
        PIN_EVENTS.append((name, self.active_path, 'order', items, text))

    def sql(self, section, name):
        blocks = re.findall(r'```sql\n(.*?)\n[ \t]*```', section, re.S)
        self.assertTrue(bool(blocks), f'Document pin {name}: SQL block missing')
        return '\n'.join(blocks)

    def row(self, table, key):
        matches = [line for line in table.splitlines()
                   if line.startswith('|') and key in line.split('|')[1]]
        self.assertTrue(len(matches) == 1, f'Document input row {key}: missing or duplicate')
        return matches[0]

    def test_two_pass_inputs_and_signature_recovery(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        first = self.scope(doc, '### Pass 1 inputs', '### Execute: pass 1')
        second = self.scope(doc, '### Pass 2 inputs', '### Execute: pass 2')
        first_values = {
            'Ordinary deployment inputs': 'Project, region, config URI, existing credential file, operator/deployer members, organization and notification channel; see [Bootstrap inputs](../../deploy/iam.md#bootstrap-inputs)',
            'PMAX_PARITY_LOCAL_CONFIRMED': "Set only after executing phase 80's printed LOCAL command successfully; stop and continue if not yet available",
            'PMAX_PARITY_DATE': 'A complete data day used by that LOCAL parity run; refresh if the calendar changed',
            'PMAX_MIGRATION_REVIEWED': 'Schema and retention review completed above',
            'PMAX_CONFIRMED_PHASES': 'Off a TTY: `10-apis,40-iam,45-wif,55-invoker,68-migration`; operator confirms each printed human-run change',
            'PMAX_SIGNED_REVIEW': 'Unset. Run pass 1 with stdin closed (`</dev/null`), or leave the phase-85 YAML prompt empty on a TTY, so it stops without validating a signature',
            'PMAX_RETENTION_CONFIRMED': '`never` on pass 1 whenever storage is incremental, even when the option map is already NULL; window mode leaves confirmation to pass 2',
            'PMAX_IMAGE_REF': 'May remain unset for eligible automatic reuse of an unfinished pass-1 generation; an explicit digest always wins on unsigned and signed invocations and bypasses the age and repository-HEAD checks; pin the reviewed digest for the signed pass',
            'PMAX_FORCE_BUILD': 'With `PMAX_IMAGE_REF` unset and no signature, deliberately start a fresh generation and build; this is the escape after automatic reuse refuses for a mismatched `repository_head_commit` or a generation older than seven days, and has no effect on explicit image selection',
            'PMAX_LOOKER_PROBE_EXPIRES_AT': 'Reviewed future UTC `YYYY-MM-DDTHH:MM:SSZ`, covering both passes, the signature delay and pass-2 phase-88 probes; missing, malformed or past values refuse',
            'PMAX_TAKE_SNAPSHOTS': 'Optional seven-day snapshots of marts base tables only; raw, ops, reporting and landing tables are not snapshotted; record the date',
        }
        for key, value in first_values.items():
            self.pin('pass1.' + key, self.row(first, key), value)
        second_values = {
            'Pass-1 inputs': 'Preserve reviewed config and human-run confirmations; refresh both credentials',
            'PMAX_ANCHOR_CONFIG': 'Saved `config-pre-v2.1.0.yaml`, matching project/raw/ops/verify/accounts and excluding cohort day 0',
            'PMAX_NOTIFICATION_CHANNEL': 'Verified operator notification channel',
            'PMAX_IMAGE_REF': 'Full reviewed image reference including digest; mandatory with a signature, no build',
            'PMAX_SIGNED_REVIEW': 'Operator-authored YAML bound to pass-1 evidence',
            'PMAX_RETENTION_CONFIRMED': 'Exact echoed `reporting_window_days + 1`, or `never` for incremental',
            'PMAX_PARITY_LOCAL_CONFIRMED': 'Set only after the LOCAL parity run for the explicit pass-2 date succeeded against the pinned digest, before launching pass 2',
            'PMAX_PARITY_DATE': 'Keep the pass-1 date explicitly, or run LOCAL parity for a fresh complete day with `PMAX_IMAGE_DIGEST` set to the pinned reference before launch; never leave this unset on pass 2',
            'PMAX_ALERT_CONFIRMED': 'Set only after proof and both source records validate for the active ladder generation and the operator has verified its pass-2 failed-execution email; the unlisted phase-95 TTY yes sets this after verification; required before listed 95 can pass phase 00',
            'PMAX_SKIPPED_ALERT_SILENT': 'Set only after proof and both source records validate for the active ladder generation and the operator has verified silence for that generation\'s preserved pass-1 SKIPPED execution; required with the email confirmation before resume',
            'PMAX_CONFIRMED_PHASES': 'Off a TTY, add `89-retention` to the pass-1 list; leave 95 unlisted for the run that produces proof, then verify both observations and add `95-resume` with both flags on re-entry; on a TTY omit 89 for the day/count check and omit 95 for its email/silence prompt; a listed 95 always requires existing proof and both flags at phase 00; never list `85-review`',
            'PMAX_LOOKER_PROBE_EXPIRES_AT': 'The SAME future UTC value as pass 1, still covering phase 88; extending it adds a second conditional binding requiring cleanup',
            'PMAX_ANCHOR_CHECKOUT': "Fresh clean verification clone at the deployment's recorded anchor commit; never the separate rollback execution clone into which current deployment records are copied",
            'PMAX_TAKE_SNAPSHOTS': 'Refresh the seven-day marts-base-table snapshots on a different pass-2 day; raw, ops, reporting and landing tables remain outside snapshot scope',
        }
        for key, value in second_values.items():
            self.pin('pass2.' + key, self.row(second, key), value)
        retry = self.scope(doc, '### Refusal and signature recovery')
        self.pin('retry.genuine', retry,
                 'For a genuine upgrade, a refusal after 85 costs one more signature: '
                 'settle the cause, refresh both credentials, repeat phases 68 to 80 '
                 'through the ladder to the next review stop, sign the refreshed '
                 'evidence, then re-run with all pass-2 inputs.')
        self.pin('retry.never_yes', retry, 'Never use `--yes`.')
        self.absent('retry.no_yes_command', '\n'.join(re.findall(r'```(?:bash|sh)\n(.*?)\n```', retry, re.S)), '--yes')
        self.pin('retry.fresh_continuation', retry,
                 'A fresh deployment has a separate continuation rule: same-digest SUCCESS '
                 'first-run evidence keeps the window default and skips the upgrade-only observation and anchor branches '
                 'until phase 95 completes the active ladder generation.')
        self.pin('retry.generation_bound_completion', retry,
                 'The completion record `resume-evidence-<digest>.json` contains both digest and generation; '
                 "a previous generation's same-digest marker cannot complete a new generation.")
        self.pin('retry.same_signature', retry,
                 'If the signed fresh-deployment pass fails at 88, 89, 90 or 95, retry with '
                 'the same signature and no anchor inputs; phase 85 revalidates the same evidence.')
        execute = self.scope(doc, '### Execute: pass 1')
        self.pin('pass1.stdin', execute, '--upgrade </dev/null')
        self.pin('pass1.phase80_stop', execute, 'The first invocation stops at phase 80 with the printed LOCAL parity command.')
        self.pin('pass1.pin_digest', execute, 'Check that `previous-image.txt` still names the pre-upgrade image, not the candidate.')
        self.pin('pass1.reuse_rule', execute, 'automatic reuse of an unfinished generation requires its `repository_head_commit` to equal `git rev-parse HEAD` in the repository containing the product root and its `started_at` to be between zero and seven days old.')
        self.pin('pass1.force_build', execute, 'A failed age or repository-HEAD check refuses automatic reuse with `PMAX_FORCE_BUILD=1` guidance; that escape applies only while the explicit reference is unset.')
        self.pin('pass1.explicit_wins', execute, 'An explicit `PMAX_IMAGE_REF` always wins, unsigned or signed, and bypasses both the age and repository-HEAD checks without rebuilding; the signature and recorded continuation identity still bind the pass.')
        self.absent('pass1.no_age_check_on_explicit', execute, 'remains subject to the same source and age checks')
        self.pin('pass1.precheck_inside75', execute, 'Positive Looker table-read control is the last step of phase 75; a refusal stops phase 75; require green before signing')
        self.pin('pass1.review_stop', self.row(execute, '85'), 'stdin closed')
        execute2 = self.scope(doc, '### Execute: pass 2')
        self.pin('pass2.tty_stdin', execute2, 'For the TTY route, remove `</dev/null` and leave 95 unlisted so phase 95 can prompt after proof exists; verify the pass-2 email and recorded pass-1 silence before answering yes.')
        self.pin('pass2.non_tty_two_invocations', execute2, 'For the non-TTY route, leave 95 unlisted while phase 90 produces proof and the ladder stops before resume; verify both observations before the later invocation with both flags set and 95 listed.')
        self.pin('pass2.precheck_inside75', self.row(execute2, 'inside 75'),
                 'Repeated as the last step of phase 75; a refusal stops phase 75')
        self.pin('pass2.repeats70', execute2, 'Phase 70 nevertheless runs its full work again: upgrade rebuild plus publish in window mode, and rebuild/publish, supervised incremental runs and the initial-history rebuild in incremental mode.')
        for value in ('rollback-anchor.txt', 'anchor_source_commit', 'anchor_public_tag', 'anchor_public_commit'):
            self.pin('anchor.record.' + value, execute2, value)
        self.pin('anchor.verifies_own_record', execute2, 'Phase 88 verifies the clean checkout against that record.')
        self.pin('pass1.reuses_probe_grant', execute, 'Reuse the same probe expiry for both passes and retries.')
        self.pin('pass1.removes_stale_grant', execute, 'remove the stale conditional binding at hand-over using its exact recorded condition, without removing the still-needed current grant')
        self.absent('anchor.no_private_image', doc, 'sha256:')
        # Lookarounds instead of word boundaries: a bare 64-character digest or a
        # 41-plus character run has no word boundary inside it and must be caught.
        hex_pattern = r'(?<![0-9a-zA-Z])[0-9a-f]{7,}(?![0-9a-zA-Z])'
        hex_tokens = set(re.findall(hex_pattern, doc))
        self.assertTrue(hex_tokens <= PUBLIC_HEX_PLACEHOLDERS,
                        'Document pin anchor.no_hex_identifier: undocumented hex literal')
        PIN_EVENTS.append(('anchor.no_hex_identifier', self.active_path,
                           'hex_tokens', hex_pattern, doc))

    def test_guards_precede_externally_visible_steps(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        for name, end in (('resource scope', '### Execute: pass 1'),
                          ('schema and readers', '### Execute: pass 1'),
                          ('retention', '### Execute: pass 2'),
                          ('resume', '### Execute: pass 2')):
            self.ordered('guard.order.' + name, doc, ('### Guard: ' + name, end))
        resource = self.scope(doc, '### Guard: resource scope')
        self.pin('guard.authorize_creation', resource,
                 'The operator verifies the project, billing, `app=pmax` label, parent '
                 'organization, region, exact account allowlist, timezone, private buckets '
                 'and all configured dataset names before permitting dataset creation in phase 20.')
        self.pin('guard.refresh_each_pass', resource,
                 'Refresh both the CLI login and local Application Default Credentials '
                 'immediately before each pass')
        self.pin('guard.adc_same_operator', resource, 'Both credentials must identify the same operator account before each pass.')
        self.pin('guard.adc_match_only', resource, 'principal with the CLI account in memory and record only match or mismatch.')
        self.pin('guard.adc_fail_closed', resource, 'A mismatch or an unavailable principal is NO-GO.')
        schema = self.scope(doc, '### Guard: schema and readers')
        self.pin('guard.zero_readers', schema, 'Zero readers is required before phase 68')
        self.pin('guard.eight_views', schema,
                 'Verify the eight old views are present in `INFORMATION_SCHEMA.VIEWS`:')
        self.pin('guard.save_before_ddl', schema,
                 'Before any view drop or column addition, save the anchor image and its source '
                 'checkout, validate the saved config, record the observation triple, inspect '
                 'the six existing cohort schemas, and have a recovery-table location ready.')
        retention = self.scope(doc, '### Guard: retention')
        self.pin('retention.static_before_pass1', retention, 'Before pass 1, review the static managed retention inventory and run Q2 and Q3 against existing datasets only.')
        self.pin('retention.preview_after20', retention, 'At the first phase-80 stop, run the full live preview with the validated `CONFIG_LOCAL`; reporting now exists and no phase-89 expiration has occurred.')
        self.pin('retention.incremental_preview', retention, 'In incremental mode, phase 68 runs the full live preview after phase 20 and before its clearing ALTERs.')
        self.absent('retention.no_early_preview', retention,
                    'Before either pass, preview the config-derived retention map:')
        self.pin('retention.no_reporting_prerequisite', retention, 'An anchor deployment may have no reporting dataset yet; do not require the full live preview before phase 20 creates it.')
        self.pin('retention.storage_change_ladder', retention, 'A storage change on an already-upgraded v2.1.0 deployment is another v2.1.0 ladder run; it does not route through the v2.0.3 rollback appendix.')
        self.pin('retention.loss_refresh', retention, 'Recompute Q1 and Q4 with the pass-2 `ALTER_DAY` immediately before launch and accept the live loss count again before phase 89.')
        self.pin('retention.loss_two_dates', retention, 'Record both planning and accepted values.')
        self.pin('retention.metadata_not_fallback', retention, "Independently read dataset metadata and record the effective value and its source as above; an absent field in a successful response is not a NO-GO.")
        self.pin('retention.metadata_call_failure', retention,
                 'A failed dataset-metadata call is NO-GO.')
        self.pin('retention.metadata_or_default', retention,
                 'If a successful response omits `maxTimeTravelHours`, record the documented default '
                 'of 168 hours with `time_travel_hours_source: default`; when present, record '
                 'its value with `time_travel_hours_source: metadata`.')
        self.pin('retention.tty_day_check', retention,
                 'On a TTY, leave `89-retention` out of `PMAX_CONFIRMED_PHASES`; the phase-89 '
                 'prompt is the pause for a new-day recomputation before any ALTER.')
        self.pin('retention.non_tty_no_stop', retention,
                 'Off a TTY there is no stop between phases 88 and 89: phase 00 requires '
                 '`89-retention` in the list and phase 89 runs right after 88')
        self.pin('retention.non_tty_post_alter_count', retention,
                 'record the post-ALTER count read from `INFORMATION_SCHEMA.PARTITIONS` as the '
                 'accepted loss (each crossed boundary adds one expired click day per table); '
                 'no re-invocation is needed.')
        self.absent('retention.no_stop_before_89_off_tty', retention, 'stop before phase 89 and re-invoke')
        self.pin('retention.static_inventory_config', retention,
                 'config = load_config(sys.argv[1])')
        self.pin('retention.static_inventory_manifest', retention,
                 "manifest = load_manifest(Path(pmax_pack.__file__).parent / 'manifest.yaml')")
        self.pin('retention.static_inventory_print', retention,
                 'print(json.dumps(expected_retention(config, manifest), indent=2, sort_keys=True))')
        self.pin('retention.marts_then_raw', retention,
                 'Phase 89 applies only differing values, marts first, raw last.')
        self.pin('retention.null_start', retention,
                 'For an anchor upgrade the live managed option map starts all NULL.')
        self.pin('retention.own_deadline', retention, "Compute the deadline from those recorded values, using the earlier operator-derived deadline if the ladder's record differs.")
        resume = self.scope(doc, '### Guard: resume')
        self.pin('resume.triple_gate', resume,
                 'On upgrades, the phase-95 observation gate compares its after reading with the preserved pass-1 baseline and records pass 1, active pass 2 and after readings before resuming Scheduler.')
        self.pin('resume.tty_email', resume, 'On a TTY, verify the pass-2 failure email and pass-1 SKIPPED silence at the phase-95 prompt before answering yes.')
        self.pin('resume.non_tty_email', resume, 'Off a TTY, the unlisted human-run phase stops before resume; verify both observations, then set both flags to `1`, list `95-resume`, and re-enter with the same reviewed digest and applicable signed evidence.')
        self.pin('resume.listed_95_requires_proof', resume, 'Whenever `95-resume` is listed, phase 00 requires valid existing proof and both flags, on and off a TTY. Either flag set without proof refuses.')
        self.pin('resume.missing_email_paused', resume, 'A missing email keeps Scheduler paused.')
        self.pin('resume.plan_readonly_generation', resume, '`--plan` resolves the active ladder generation read-only before phase 00 and validates alert-proof against it as the live pass does')
        self.pin('resume.alert_archive', resume, 'builds and validates the new record in a temporary file, preserves the current record\'s exact bytes beside it as `<record name without .json>-<prior generation>.json`, records that archived path once')
        self.pin('resume.generation_bound_alert_records', resume, 'The current `alert-proof-<digest>.json`, `alert-submission-<digest>.json` and `lease-drill-evidence-<digest>-pass1.json` each carry the active ladder generation.')
        self.pin('resume.older_generation_proof_refused', resume, "A retry may reuse a same-generation submission or proof, but no previous generation's record can authorize resume.")
        self.absent('resume.no_launch_commitment', resume, 'commitments at pass-2 launch')

    def test_rollback_order_and_recovery_table(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        rollback = self.scope(doc, '## Rollback appendix')
        self.ordered('rollback.steps', rollback,
                     ('1. Pause Scheduler', '2. Stop expiration',
                      '3. Drop the added column', '4. Restore the saved config',
                      '5. Run the anchor checkout'))
        self.pin('rollback.recovery_before_anchor', rollback,
                 'Any expired-row recovery runs after step 2 and before step 5, inside the recorded deadline.')
        self.pin('rollback.anchor_waits', rollback, 'The anchor ladder waits for recovery and reconciliation;')
        self.pin('rollback.twin_drop', rollback, 'Also run `DROP TABLE IF EXISTS` on the same six cohort table names in the marts verify twin only, before the anchor ladder.')
        self.pin('rollback.fresh_clone', rollback, "Run the anchor checkout's own ladder from a fresh rollback execution clone at the recorded anchor commit")
        self.pin('rollback.copy_deployment', rollback, 'deployment.yaml')
        self.pin('rollback.copy_pointer', rollback, 'previous-image.txt')
        self.pin('rollback.copy_baseline', rollback, 'observation-before.json')
        self.pin('rollback.frozen', rollback, 'Dashboards remain at the last published as_of')
        recover = self.scope(doc, '### Recover expired rows before rollback_deadline')
        self.pin('rollback.labelled_capped_runner', rollback, 'Run every printed NULL ALTER, six live DROP COLUMN statements, disposable twin DROP TABLE, recovery CTAS and recovery transaction through the labelled runner in Evidence query recipes, with a bytes cap sized to the recovered range.')
        self.pin('recovery.dataset_eu', rollback, "Set `RECOVERY` to the incident's named EU dataset (for example `pmax_recovery`), labeled `app=pmax` and the config `env`, with no grant to pmax-looker.")
        self.pin('recovery.inventory_copies', rollback, 'Inventory its recovery tables in the incident record for deletion after reconciliation; creating a recovery dataset requires the earlier scope review and operator authorization.')
        self.pin('recovery.utc_binding', recover, 'before_alter:TIMESTAMP:')
        self.pin('recovery.cutoff_binding', recover, 'cutoff:DATE:')
        self.pin('recovery.offset_reason', recover, 'An unqualified `FOR SYSTEM_TIME AS OF` timestamp defaults to America/Los_Angeles, so the explicit UTC binding prevents a shifted recovery.')
        self.pin('recovery.dataset_scope', rollback, 'with no grant to pmax-looker.')
        self.pin('recovery.labels', recover, '--label=stage:audit')
        self.pin('recovery.cap', recover, '--maximum_bytes_billed=')
        sql = self.sql(recover, 'recovery.sql')
        self.pattern('recovery.materialize_before_transaction', sql,
                     r'CREATE TABLE\s+`PROJECT\.RECOVERY\.recovered_table`\s+AS\s+SELECT\s+\*\s+'
                     r'FROM\s+`PROJECT\.DATASET\.TABLE`\s+FOR SYSTEM_TIME AS OF\s+@before_alter\s+'
                     r'WHERE\s+click_date\s*<\s*@cutoff\s*;\s*BEGIN TRANSACTION;')
        self.pattern('recovery.insert_from_copy', sql,
                     r'INSERT INTO\s+`PROJECT\.DATASET\.TABLE`\s*\([^;]+?\)\s+SELECT\s+[^;]+?'
                     r'FROM\s+`PROJECT\.RECOVERY\.recovered_table`\s*;\s*COMMIT TRANSACTION;')
        self.forbidden_sql('recovery.no_own_history_anywhere', sql,
                           r'INSERT\s+INTO\s+`(?P<target>[^`]+)`[^;]*?'
                           r'(?:FROM|JOIN)\s+`(?P=target)`\s+FOR SYSTEM_TIME AS OF')
        self.absent('recovery.no_history_inside_transaction',
                    sql.split('BEGIN TRANSACTION;')[-1], 'FOR SYSTEM_TIME AS OF')
        self.pin('recovery.no_false_restore', recover, 'never claim NULL restored the rows')
        self.pin('reupgrade.twin_drop', recover, 'For a re-upgrade, phase 88 automatically drops the six disposable cohort tables in the marts verify twin once before rebuilding the candidate schema.')
        self.pin('reupgrade.no_recreation', recover, 'without another whole-table drop or recreation from anchor DDL. No live table is dropped by this twin cleanup.')

    def test_rollback_preserves_cohort_reading_before_column_drop(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        rollback = self.scope(doc, '## Rollback appendix')
        self.ordered('rollback.cohort_order', rollback,
                     ('2. Stop expiration', '2a. Preserve the pre-migration cohort reading',
                      '3. Drop the added column', '4. Restore the saved config',
                      '5. Run the anchor checkout'))
        for name, needle in (
            ('capture_before_drop', 'Materialize every recovery CTAS immediately after step 2, while cohort_counting still exists; run its DELETE and INSERT only after step 3, with explicit columns omitting cohort_counting.'),
            ('window', "Compute anchor_window_start as max(anchor.start_date, rollback as_of minus requested_days), where requested_days is the longest complete live click-through lookback plus anchor.restatement_margin_days or max(anchor.cohort_days) plus that margin when Family-D evidence is absent or incomplete."),
            ('evidence', 'Before dropping the marker, save the following evidence separately for each of the six cohort tables.'),
            ('restore', 'For click dates older than the anchor window start, restore the pre-migration contents from time travel or an eligible phase-25 marts snapshot.'),
            ('before_migration', 'For cohort recovery, choose before_alter strictly before the first ladder-68 ALTER, even when the retention record names a later expiration ALTER.'),
            ('snapshot', 'A snapshot is eligible only if it was taken before that first migration ALTER; a refreshed pass-2 snapshot may already contain the new reading.'),
            ('gap', 'Outside time travel and without an eligible snapshot, copy only the marked rows older than the anchor window start to the recovery dataset, then delete those same rows from live before step 3.'),
            ('gap_notice', 'Record the missing click-date range and counts in both the incident record and the consumer notice.'),
            ('assumption', 'Stated assumption: gaps are preferable to silently shifted values.'),
            ('recovery_order', 'After step 3, run only the replacement transaction below, projecting the exact remaining target columns from the already materialized recovery table.'),
        ):
            self.pin('rollback.cohorts.' + name, doc.replace('`', ''), needle)
        self.pin('rollback.cohorts.recorded_scope', doc,
                 'The phase-88 record names anchor_window_uncovered_days as a config-derived reference band, with anchor_window_uncovered_days_source set to config.')
        self.pin('rollback.cohorts.actual_scope', doc,
                 "The live uncovered band can be smaller or larger than this reference band; recompute the actual boundary at rollback.")
        self.pin('rollback.cohorts.render_unchanged', doc,
                 "Phase 88 still renders the anchor scripts over the candidate reporting window, so its success does not prove that a real anchor run will rewrite that entire interval.")
        sql = self.sql(rollback, 'rollback.cohort_evidence')
        for table in ('int_lag_prefix_campaign', 'int_lag_prefix_asset_group',
                      'int_observation_cells', 'mart_cohort_campaign',
                      'mart_cohort_asset_group', 'mart_cohort_asset'):
            self.pin('rollback.cohorts.table.' + table, sql, 'PROJECT.MARTS.' + table)
        for expression in ('GROUP BY cohort_counting', 'MIN(click_date)', 'MAX(click_date)',
                           'COUNT(*)', "label.key = 'stage' AND label.value = 'ladder-68'",
                           "statement_type = 'ALTER_TABLE'", 'INFORMATION_SCHEMA.JOBS_BY_PROJECT',
                           'cohort_counting IS NOT NULL AND click_date < @anchor_window_start'):
            self.pin('rollback.cohorts.sql.' + expression, sql, expression)
        book = self.document('RUNBOOK.md', private=True)
        mirror = self.scope(book, '### Upgrade rollback')
        self.ordered('rollback.mirror_order', mirror,
                     ('Materialize recovery CTAS', 'Then drop `cohort_counting`',
                      'Run the recovery DELETE and INSERT', 'Restore the saved'))
        for needle in ('gaps are preferable to silently shifted values',
                       'incident record and the consumer notice',
                       'anchor_window_uncovered_days', 'longest live click-through lookback'):
            self.pin('rollback.mirror.' + needle, mirror, needle)

    def test_recovery_source_and_quarantine_verification_order(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        quarantine = self.scope(doc, 'Outside time travel and without an eligible snapshot,', '3. Drop the added column')
        self.ordered('rollback.quarantine_check_before_delete', quarantine,
                     ('CREATE TABLE `PROJECT.RECOVERY.quarantined_cohort_table`',
                      "In one script, assert that the recovery copy's row count equals the live predicate's row count immediately before BEGIN TRANSACTION, with DELETE following only after the assertion succeeds.",
                      'ASSERT (SELECT COUNT(*)', "AS 'quarantine copy incomplete';", 'BEGIN TRANSACTION;', 'DELETE FROM `PROJECT.MARTS.COHORT_TABLE`', 'COMMIT TRANSACTION;'))
        self.pattern('rollback.quarantine_same_scope', self.sql(quarantine, 'quarantine.sql'),
                     r'CREATE TABLE[^;]+WHERE cohort_counting IS NOT NULL AND click_date < @anchor_window_start;'
                     r'.*DELETE FROM[^;]+WHERE cohort_counting IS NOT NULL AND click_date < @anchor_window_start;')
        for name, needle in (
            ('jobs_binding', 'Find that first migration timestamp in the labelled JOBS view, binding migration_start_time to the recorded start of this upgrade; a missing timestamp stops time-travel recovery until resolved.'),
            ('jobs_deadline', 'Bind migration_start_time as an explicit UTC TIMESTAMP in the evidence runner and verify the returned value is still inside the time-travel window before using it as before_alter in the recovery CTAS.'),
            ('snapshot_copy', 'For snapshot recovery, materialize the same older click-date range from that saved snapshot into the recovery table instead of the historical live-table read, then use the same replacement transaction after step 3.'),
            ('quarantine_per_table', 'Bind anchor_window_start as a DATE and run this separately for each named cohort table, with a distinct recovery table per source:'),
            ('cohort_recovery_projection', 'For cohort reading recovery, bind cutoff to the anchor window start in both files and omit cohort_counting from both INSERT and SELECT columns.'),
            ('timestamp_source', 'For cohort recovery, use the pre-migration timestamp from step 2a even when no retention record exists; for other expired rows, use a timestamp before the first expiration ALTER recorded for that table.'),
        ):
            self.pin('rollback.detail.' + name, doc.replace('`', ''), needle)
        recovery = self.scope(doc, '### Recover expired rows before rollback_deadline')
        self.pin('rollback.runner_source', recovery,
                 'before_alter:TIMESTAMP:<UTC ISO with Z, before first ladder-68 ALTER for cohorts or first retention ALTER otherwise>')
        self.absent('rollback.no_late_cohort_timestamp', recovery,
                    "strictly before the table's first ALTER in retention-<digest>.json>")
        q7 = self.scope(doc, '**Q7, closed-month stability.**', '**Legacy-view inventory.**')
        for needle in ('--parameter="closed_month_end:DATE:<ISO first day of month>"',
                       'Repeat for raw `volume_campaign`, `volume_asset_group`, and `volume_asset`:',
                       'Also record row counts for raw `conv_campaign`, `conv_asset_group`, `conv_asset`, `lag_campaign`, and `lag_asset_group`; these fact schemas have no cost column, so replace `SUM(cost_micros)` with `CAST(NULL AS INT64)` and record cost as not applicable, never as an inferred zero.',
                       'Replace `FACT_TABLE` with each literal table name; all eight raw fact tables use `date` as their partition column.',
                       'Run this through the labelled, bytes-capped evidence runner, adding'):
            self.pin('closed_month.detail.' + needle, q7, needle)
        book = self.document('RUNBOOK.md', private=True)
        for needle in ('See the migration Q7 recipe and preserve both readings with the Go/No-Go evidence.',
                       'While `cohort_counting` exists, save per-table marker counts and minimum and maximum click dates for all six cohort tables, using the migration appendix.',
                       'Compute `requested_days` as the chosen lookback plus `anchor.restatement_margin_days`, then `anchor_window_start` as `max(anchor.start_date, rollback as_of minus requested_days)`; restore pre-migration rows before that start from a time-travel copy made before the first ladder-68 ALTER or an eligible phase-25 snapshot.',
                       'Without either source, copy marked rows before that start into recovery and delete those exact live rows before dropping the marker: gaps are preferable to silently shifted values.',
                       'Run the recovery DELETE and INSERT after the column drop, with explicit remaining columns that omit `cohort_counting`.',
                       'The phase-88 `anchor_window_uncovered_days` is a config-derived reference band, with `anchor_window_uncovered_days_source` set to `config`; the live band can be smaller or larger, so recompute the boundary at rollback.',
                       'The anchor rehearsal still uses the candidate reporting window.'):
            self.pin('rollback.mirror.detail.' + needle, book, needle)

    def test_closed_month_stability_is_required_in_both_storage_modes(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        self.pin('closed_month.before_step0', self.scope(doc, '## Step 0: operator preparation'),
                 'Before step 0 changes anything, save Q7 for every fact table; repeat it after phase 70 using the same closed-month boundary.')
        self.pin('closed_month.go', self.scope(doc, '## Go/No-Go and after-invariants'),
                 'Q7 requires exact row-count and cost-sum equality for closed months wholly older than the phase-70 re-extraction start; record and resolve every difference in intersecting closed months before GO, in both incremental and window modes.')
        q7 = self.scope(doc, '**Q7, closed-month stability.**', '**Legacy-view inventory.**')
        for needle in ('For each fact table, save row counts and SUM of cost by closed month before step 0 and after phase 70.',
                       'Bind the same closed_month_end DATE in both reads to the first day of the month containing the pre-upgrade as_of.',
                       'Compare the union of before and after month keys, including missing or newly populated months; treat a missing month as zero rows and zero additive cost for comparison, preserving its missing status in the evidence.',
                       'Require exact row-count and applicable cost-sum equality only for closed months whose end-exclusive date is on or before the phase-70 re-extraction start.'):
            self.pin('closed_month.rule.' + needle, q7.replace('`', ''), needle)
        for needle in ('DATE_TRUNC(date, MONTH) AS closed_month', 'COUNT(*) AS row_count',
                       'SUM(cost_micros) AS cost_micros', 'WHERE date < @closed_month_end',
                       'GROUP BY closed_month', 'ORDER BY closed_month'):
            self.pin('closed_month.sql.' + needle, self.sql(q7, 'closed_month'), needle)
        book = self.document('RUNBOOK.md', private=True)
        self.pin('closed_month.private', book,
                 'Before resume, Q7 compares every fact table before step 0 and after phase 70 in both incremental and window modes: exact equality applies only to closed months whose end-exclusive date is on or before the phase-70 re-extraction start (phase-70 as_of minus reporting_window_days).')

    def test_asset_reason_column_rename_and_agent_boundaries(self):
        for path in ('docs/migrations/v2.1.0.md', 'docs/releases/v2.1.0.md'):
            doc = self.document(path)
            self.pin('asset_reasons.rename', doc,
                     "`v_asset_performance.asset_primary_status_reasons` (ARRAY<STRING>) becomes `pmax_reporting.asset_performance.asset_primary_status_reasons_text` (STRING joined with ', '), so UNNEST filters become string matches.")
        for path in ('AGENTS.md', 'RUNBOOK.md'):
            doc = self.document(path, private=True)
            for needle in ('twin-only apply under agent-safe phase 88',
                           'live apply only from human-run phases 68 and 89',
                           'Never run `--apply` by hand', 'never author `PMAX_RETENTION_CONFIRMED`',
                           '`--phase-88-record`', '`PMAX_IMAGE_DIGEST`',
                           'operator-only: an intentional reporting-date rollback; on an older-as-of refusal, stop and report'):
                self.pin('agent_boundary.' + needle, doc, needle)
        for path in ('docs/operations.md', 'docs/cohorts.md'):
            doc = self.document(path)
            self.pin('rebuild.guard_placement', doc,
                     'For a real production rebuild, the older-as-of check runs once after lease acquisition and before the score stage or any mart rewrite; a verification rebuild takes no lease and runs the check first, also before the score stage or any mart rewrite.')
            self.pin('rebuild.guard_cached', doc,
                     'The publish stage records that decision without repeating the check; the production lease excludes a concurrent publish.')

    def test_anchor_boundary_uses_complete_live_evidence_and_saved_config(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        rollback = self.scope(doc, '2a. Preserve the pre-migration cohort reading', '3. Drop the added column')
        for name, needle in (
            ('inputs', 'In the saved anchor checkout, load the saved anchor config with its own config parser at rollback as_of and record start_date, restatement_margin_days, max(cohort_days), and the complete configured accounts list.'),
            ('account_selection', "The evidence query binds the configured account list, which equals the anchor's resolved set in allowlist mode; when the saved anchor config sets bulk_expansion: true, bind the resolved account set recorded by the last successful anchor run instead (the resolved_accounts field of that run's report), since the anchor expands the MCC at run time, or use the config fallback when that record is unavailable."),
            ('runner', 'Run the following single evidence query through the labelled, bytes-capped runner, binding as_of as the rollback DATE and anchor_accounts as ARRAY<STRING> containing every account in the selected set.'),
            ('complete', 'Use longest_live_lookback_days only when account_count equals the nonzero selected account count bound in anchor_accounts and the value is positive; otherwise the Family-D evidence is incomplete, so use max(anchor.cohort_days) as the lookback fallback.'),
            ('reference_formula', 'anchor_window_uncovered_days = max(0, W - (max(anchor.cohort_days) + anchor.restatement_margin_days))'),
            ('inputs_evidence', 'Save the query job ID, result, resolved anchor inputs, chosen lookback source, rollback as_of, and recomputed anchor_window_start with the incident evidence.'),
        ):
            self.pin('rollback.anchor.' + name, rollback.replace('`', ''), needle)
        self.pin('rollback.anchor.effective_boundary', rollback.replace('`', ''),
                 'Add the saved margin to the chosen lookback to obtain requested_days, then subtract that depth from rollback as_of and use the later of that date and the saved start_date.')
        self.pin('rollback.anchor.missing_source', rollback.replace('`', ''),
                 'When the source tables are absent, record that condition and use the config fallback; any other query error stops this recipe.')
        self.absent('rollback.anchor.no_reporting_field', rollback, 'reporting_window_days')
        evidence = self.sql(rollback, 'rollback.anchor.sql')
        for name, needle in (
            ('load_stage', "WHERE stage = 'load' AND account_id IS NULL"),
            ('snapshot_as_of', 'WHERE c.snapshot_date <= @as_of'),
            ('latest_stage', 'PARTITION BY run_id ORDER BY event_ts DESC'),
            ('success', "WHERE status = 'SUCCESS'"),
            ('account_filter', 'CAST(c.account_id AS STRING) IN UNNEST(@anchor_accounts)'),
            ('latest_snapshot', 'PARTITION BY c.account_id ORDER BY c.snapshot_date DESC, c.run_id DESC'),
            ('action_identity', 'ON a.account_id = s.account_id AND a.snapshot_date = s.snapshot_date AND a.run_id = s.run_id'),
            ('positive_evidence', 'WHERE a.click_through_lookback_window_days IS NOT NULL'),
            ('per_account_max', 'MAX(a.click_through_lookback_window_days) AS max_window_days'),
            ('longest', 'MAX(max_window_days) AS longest_live_lookback_days'),
            ('account_count', 'COUNT(DISTINCT account_id) AS account_count'),
        ):
            self.pin('rollback.anchor.sql.' + name, evidence, needle)
        self.absent('rollback.anchor.no_invented_lower_bound', evidence, '37 MONTH')
        book = self.document('RUNBOOK.md', private=True)
        for name, needle in (
            ('inputs', 'Read start_date, restatement_margin_days, max(cohort_days), and all configured accounts with the saved anchor checkout and config parser at rollback as_of.'),
            ('missing_source', 'When the source tables are absent, record that condition and use the config fallback; any other query error stops this recipe.'),
            ('account_selection', "The evidence query binds the configured account list, which equals the anchor's resolved set in allowlist mode; when the saved anchor config sets bulk_expansion: true, bind the resolved account set recorded by the last successful anchor run instead (the resolved_accounts field of that run's report), since the anchor expands the MCC at run time, or use the config fallback when that record is unavailable."),
            ('query', 'Use the migration appendix labelled, bytes-capped Family-D query for the longest live click-through lookback; require every account in the selected set bound in anchor_accounts and a positive result, falling back to max(anchor.cohort_days) only for incomplete evidence.'),
            ('reference', 'anchor_window_uncovered_days = max(0, W - (max(anchor.cohort_days) + anchor.restatement_margin_days))'),
        ):
            self.pin('rollback.anchor.mirror.' + name, book.replace('`', ''), needle)

    def test_rollback_reference_band_has_no_retired_upper_bound_claim(self):
        for path in ('docs/migrations/v2.1.0.md', 'RUNBOOK.md'):
            doc = self.document(path, private=(path == 'RUNBOOK.md'))
            for name, wording in (('upper_bound', 'upper bound'),
                                  ('at_most', 'at most the recorded band')):
                self.absent('rollback.reference_retired.' + name, doc.lower(), wording)

    def test_quarantine_assertion_and_committed_delete_count_are_required(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        quarantine = self.scope(doc, 'Outside time travel and without an eligible snapshot,', '3. Drop the added column')
        sql = self.sql(quarantine, 'quarantine.guarded_sql')
        self.pattern('rollback.quarantine.transaction_guard', sql,
                     r"ASSERT \(SELECT COUNT\(\*\) FROM `PROJECT\.MARTS\.COHORT_TABLE`"
                     r"\s+WHERE cohort_counting IS NOT NULL AND click_date < @anchor_window_start\)"
                     r"\s*= \(SELECT COUNT\(\*\) FROM `PROJECT\.RECOVERY\.quarantined_cohort_table`\)"
                     r"\s+AS 'quarantine copy incomplete';\s*BEGIN TRANSACTION;\s*DELETE FROM")
        self.pattern('rollback.quarantine.single_script', quarantine,
                     r"```sql\s+ASSERT [^;]+AS 'quarantine copy incomplete';"
                     r"\s*BEGIN TRANSACTION;\s*DELETE FROM `PROJECT\.MARTS\.COHORT_TABLE`"
                     r"\s+WHERE cohort_counting IS NOT NULL AND click_date < @anchor_window_start;"
                     r"\s*COMMIT TRANSACTION;\s*```")
        self.pin('rollback.quarantine.assertion_failure', quarantine,
                 'Run ASSERT and the following transaction in the same script without session mode and without an exception handler; a failed assertion aborts that script before BEGIN TRANSACTION, so the DELETE never runs.')
        self.pin('rollback.quarantine.automatic_rollback', quarantine,
                 "A failed DELETE or COMMIT is rolled back automatically instead of leaving an open session holding the table's mutation lock.")
        for name, needle in (
            ('count_rule', "Record the DELETE child job's dml_statistics.deleted_row_count from INFORMATION_SCHEMA.JOBS_BY_PROJECT and require it to equal the recovery copy count."),
            ('commit_rule', 'Require exactly one successful script row, one successful COMMIT_TRANSACTION child, and one successful DELETE child for that recorded script job ID; missing, failed, duplicate, or unequal evidence stops rollback before the column drop.'),
            ('parent_caveat', "Use the DELETE child's DML statistics, never the SCRIPT parent's statistics or a sum across unrelated jobs."),
        ):
            self.pin('rollback.quarantine.' + name, quarantine.replace('`', ''), needle)
        for name, needle in (
            ('bounded_jobs', 'WHERE creation_time BETWEEN @quarantine_started_at AND @quarantine_finished_at'),
            ('script_binding', 'job_id = @quarantine_script_job_id OR parent_job_id = @quarantine_script_job_id'),
            ('script_success', "COUNTIF(job_id = @quarantine_script_job_id AND statement_type = 'SCRIPT' AND state = 'DONE' AND error_result IS NULL) AS successful_scripts"),
            ('delete_success', "COUNTIF(parent_job_id = @quarantine_script_job_id AND statement_type = 'DELETE' AND state = 'DONE' AND error_result IS NULL) AS successful_deletes"),
            ('commit_success', "COUNTIF(parent_job_id = @quarantine_script_job_id AND statement_type = 'COMMIT_TRANSACTION' AND state = 'DONE' AND error_result IS NULL) AS successful_commits"),
            ('delete_count', "MAX(IF(statement_type = 'DELETE' AND parent_job_id = @quarantine_script_job_id, dml_statistics.deleted_row_count, NULL)) AS deleted_row_count"),
            ('copy_count', '(SELECT COUNT(*) FROM `PROJECT.RECOVERY.quarantined_cohort_table`) AS recovery_copy_count'),
            ('count_equality', 'deleted_row_count = recovery_copy_count AS copy_count_matches'),
        ):
            self.pin('rollback.quarantine.sql.' + name, sql, needle)
        book = self.document('RUNBOOK.md', private=True)
        self.pin('rollback.quarantine.mirror.automatic_rollback', book,
                 "A failed DELETE or COMMIT is rolled back automatically instead of leaving an open session holding the table's mutation lock.")
        for needle in ('In the same quarantine script, without session mode and without an exception handler, place ASSERT immediately before BEGIN TRANSACTION to compare the live predicate count with the recovery copy count, with quarantine copy incomplete as the refusal; a failed assertion stops the script before the transaction opens or the DELETE runs.',
                       "Record the successful DELETE child job's dml_statistics.deleted_row_count from INFORMATION_SCHEMA.JOBS_BY_PROJECT and require equality with the copy count; verify the recorded parent script and its COMMIT_TRANSACTION child succeeded, using the bounded evidence query in the appendix before dropping the marker."):
            self.pin('rollback.quarantine.mirror.' + needle, book.replace('`', '').replace("'", ''), needle.replace("'", ''))

    def test_closed_month_boundary_and_differences_are_reviewable(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        q7 = self.scope(doc, '**Q7, closed-month stability.**', '**Legacy-view inventory.**')
        for name, needle in (
            ('start', 'Compute the phase-70 re-extraction start as its recorded as_of minus reporting_window_days from the config used for that run.'),
            ('straddle', 'A closed month that straddles this boundary intersects the re-extraction window and belongs in the review group.'),
            ('deltas', 'For every difference in an intersecting closed month, record direction and size as after minus before for rows and applicable cost; a decrease is a loss signal requiring operator investigation and resolution before GO.'),
            ('both_modes', 'Apply these rules in both incremental and window modes; retain the before and after values and operator resolution with the Q7 evidence.'),
        ):
            self.pin('closed_month.boundary.' + name, q7.replace('`', ''), needle)
        self.pin('closed_month.end_exclusive', q7,
                 'DATE_ADD(closed_month, INTERVAL 1 MONTH) <= DATE_SUB(@phase70_as_of, INTERVAL @reporting_window_days DAY)')
        self.absent('closed_month.no_retention_exception', q7, 'any deliberate retention loss')
        book = self.document('RUNBOOK.md', private=True)
        self.pin('closed_month.mirror_review', book,
                 'For closed months that intersect that window, including a month straddling its start, record every difference with direction and size (after minus before); investigate and resolve decreases as loss signals before GO, preserving operator review in the Q7 evidence.')

    def test_rollback_before_phase89_has_no_required_retention_record(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        rollback = self.scope(doc, '## Rollback appendix')
        self.pin('rollback.pre89_no_record', rollback,
                 'Before phase 89 reaches its first ALTER, a failure at rebuild, validation or '
                 'anchor rehearsal can leave no retention record.')
        self.pin('rollback.null_without_record', rollback,
                 'verify the live option map is all NULL and record that no expiration ALTER occurred')
        self.pin('rollback.write_ahead', rollback,
                 'If any phase-89 ALTER was attempted, its write-ahead record must exist even on '
                 'partial failure: use that original inventory.')
        self.pin('rollback.deadline_refusal', rollback, 'Refuse a recovery read outside that deadline.')
        self.pin('rollback.print_only', rollback, 'This is print-only.')
        self.pin('rollback.original_inventory_only', rollback,
                 'Stop expiration, when the phase-89 record exists, on exactly the '
                 '`original_inventory` in `retention-<digest>.json`.')
        self.pin('rollback.no_old_expiration', rollback, 'keep them NULL, never restore the old expiration.')

    def test_observation_baseline_matches_phase25_raw_dataset(self):
        doc = self.document('RUNBOOK.md', private=True)
        recipe = self.scope(doc, '### Config save and observation recipes')
        self.pin('observation.raw_baseline', self.sql(recipe, 'observation.baseline'),
                 'FROM `PROJECT.RAW.raw_observations`;')
        self.absent('observation.no_ops_baseline', doc, 'PROJECT.OPS.raw_observations')
        recovery = self.scope(doc, '### Upgrade rollback')
        self.pin('observation.raw_window', recovery, "If the incident is within the raw dataset's actual time-travel window, recover `raw_observations` from the configured raw dataset.")
        self.pin('observation.raw_metadata', recovery, 'Record its actual time-travel hours and `time_travel_hours_source` from the metadata recipe above, plus the pre-incident UTC timestamp, in observation-recovery evidence before the read.')
        self.pin('observation.ops_only_ops', recovery, 'The ops time-travel window applies only to tables stored in ops, never to `raw_observations`.')
        record = self.scope(doc, '### v2.1.0 reference Go/No-Go (pending)')
        self.pin('observation.preserve_pass1', record, 'record preserved pass 1 and active pass 2 in `observation-gate-<digest>.json` beside the after reading')
        self.absent('observation.no_symlink', record, 'symlink to')
        self.pin('observation.unchanged_sentence', record,
                 'The migration itself must leave the observation triple unchanged.')
        phase = (root / 'deploy/phases/25-dry-run.sh').read_text()
        self.assertTrue('${DATASET_RAW}.raw_observations' in phase,
                        'phase 25 must still read raw observations')

    def test_incremental_cost_and_config_edges(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        config = self.scope(doc, '## Step 0: operator preparation', '### Pass 1 inputs')
        self.pin('config.variables', doc, '`CONFIG_LOCAL` is the validated local config copy prepared in step 0, with explicit storage; it is not the saved pre-upgrade config.')
        self.pin('config.env_variable', doc, '`PMAX_ENV` is its `env` value.')
        self.pin('config.upload_single_sitting', config, 'The new-config upload and pass 1 through phase 70 happen in one sitting, before the next scheduled morning: the anchor cannot parse cohort day 0 and the new image needs phase 68\'s schema.')
        self.pin('config.starts_after_morning', config, "Start after that morning's scheduled run in the deployment timezone (`timezone_override`) has completed.")
        self.pin('config.scalars', config, 'An unquoted all-digit value or boolean scalar is refused.')
        self.pin('config.month_start', config,
                 'In incremental mode `start_date` must be the first day of a month;')
        self.pin('config.editor_prefix', config, '40 accepts only `user:` editors and `serviceAccount:` service agents.')
        self.pin('config.agent_prefix', config, 'The config parser accepts either principal prefix in either list, but phase 40 accepts only `user:` editors and `serviceAccount:` service agents.')
        self.pin('config.rollback_pair', config, 'current Job digest beside the recorded anchor digest')
        incremental = self.scope(doc, '### Incremental variant and wall-time budget')
        self.pin('incremental.month_count', incremental,
                 '`month_count = 12 * (E.year - S.year) + E.month - S.month + 1`')
        self.pin('incremental.probe', incremental,
                 'Phase 70 first rebuilds/publishes, then starts one supervised incremental run as the plan probe.')
        self.pin('incremental.zero_counts', incremental,
                 'Its `pending_after` is authoritative: zero ends the loop after that one execution, '
                 'which counts against `PMAX_FIRST_RUN_MAX_EXECUTIONS`')
        self.pin('incremental.history', incremental,
                 'then perform the initial-history rebuild with `--window-start=<clamped-start-date>`.')
        self.pin('incremental.repeat_budget', incremental, 'Apply this wall-time budget, credential window and supervised stop to pass 2 and every repeat of phases 68 to 80: each re-executes the full phase-70 sequence, including the supervised probe and initial-history rebuild in incremental mode.')
        self.pin('incremental.credential_budget', incremental,
                 'Set a supervised stop time within the credential window.')

    def test_private_go_no_go_has_sources_and_no_fabricated_results(self):
        doc = self.document('RUNBOOK.md', private=True)
        record = self.scope(doc, '### v2.1.0 reference Go/No-Go (pending)')
        self.pin('record.fixed_ruling', record, 'operator ruling is `storage: window`')
        self.pin('record.probe_expiry', record, 'PMAX_LOOKER_PROBE_EXPIRES_AT')
        self.pin('record.raw_loss', record, 'Raw loss planning value / pass-2 accepted value: click days / rows / bytes / boundary-day rows')
        self.pin('record.mart_loss', record, 'Mart loss planning value / pass-2 accepted value: click days / rows / bytes / boundary-day rows')
        self.pin('record.sources', record, 'Repeat migration Q1 on pass-2 ALTER_DAY before phase 89, joined to the derived ALTER list; re-accept after any day change')
        self.pin('record.metadata', record, "Migration Q2 and metadata recipe below: recorded hours and metadata/default provenance for both datasets; planned ALTER time plus smaller recorded hours")
        self.pin('record.additive', record, 'Repeat migration Q4 on pass-2 ALTER_DAY immediately before launch; enumerate actual additive columns and bind accepted cutoff')
        self.pin('record.saved_config', record, 'config-pre-v2.1.0.generation.txt')
        self.pin('record.snapshot_scope', record, 'These optional seven-day snapshots cover marts base tables only; raw, ops, reporting and landing tables are not snapshotted.')
        self.pin('record.optional_probe', record, 'Advisory one-column probe on one disposable twin table before pass 2, recorded if run')
        self.pin('record.empty_policy', record, 'NO-GO until filled')
        lines = record.splitlines()
        start = next((i for i, line in enumerate(lines) if line.startswith('| Stage / record field | Value |')), None)
        self.assertTrue(start is not None, 'Document pin record.table: missing value table')
        fields = 0
        for line in lines[start + 2:]:
            if not line.startswith('|'):
                break
            cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
            self.assertTrue(len(cells) == 3, 'Document pin record.table: expected three cells')
            field, value, source = cells
            allowed = value == '' or (value == '`storage: window`' and 'storage' in field.lower())
            self.assertTrue(allowed, 'Document pin record.blank_values: fabricated value in template')
            self.assertTrue(field in REVIEWED_GO_NO_GO_SOURCES,
                            'Document pin record.sources: unreviewed source row')
            name = f'record.source.{fields + 1:02d}'
            expected = REVIEWED_GO_NO_GO_SOURCES[field]
            with self.subTest(pin=name):
                self.assertTrue(normalized(source) == normalized(expected),
                                f'Document pin {name}: source cell changed or contains a result')
                self.assertTrue(re.search(r'\b(?:GO|NO-GO)\b', source) is None,
                                f'Document pin {name}: source cell contains a decision')
            PIN_EVENTS.append((name, self.active_path, 'source_cell', (field, expected), line))
            fields += 1
        self.assertTrue(fields == len(REVIEWED_GO_NO_GO_SOURCES),
                        'Document pin record.table: missing reviewed Go/No-Go source row')
        PIN_EVENTS.append(('record.blank_values', self.active_path, 'blank_table', '| Stage / record field | Value |', record))
        privacy = self.scope(doc, '### Operator identity and private evidence')
        self.pin('evidence.no_addresses', privacy,
                 'Every operator-written evidence file records principals by count and role, never by address.')
        self.pin('identity.adc_fail_closed', privacy, 'A mismatch or an unavailable principal is NO-GO, including missing email scope or tokeninfo failure.')
        self.pin('evidence.ladder_records_private', privacy,
                 'Ladder-written records such as `deployment.yaml` and '
                 '`retention_operator_account` can carry principal addresses and are private '
                 'by contract; do not copy those addresses into operator-written evidence.')
        self.pin('private.listed_95_requires_proof', doc,
                 'Whenever `95-resume` is listed, phase 00 requires valid existing proof and both flags, '
                 'on a TTY as well as off one. Setting either flag before proof exists refuses.')
        self.pin('private.flags_after_proof', doc,
                 'For the pass-2 run that first produces proof for the active ladder generation, leave both alert flags unset and leave `95-resume` out of that list, even if the same image digest has proof from an older generation.')
        self.pin('private.plan_readonly_generation', doc, 'plan: no active ladder generation; alert-proof validation runs on the live pass')
        self.pin('private.alert_archive', doc, 'an existing archive with identical bytes is accepted on retry, while different bytes refuse with the archive name.')
        self.pin('private.alert_archive_atomic', doc, 'atomically renames the prepared record into place')
        self.pin('private.alert_temp_cleanup', doc, 'A refused validation removes that temporary file.')
        self.pin('private.generation_bound_alert_records', doc,
                 'The current `alert-proof-<digest>.json`, `alert-submission-<digest>.json` and `lease-drill-evidence-<digest>-pass1.json` each carry a `generation` field.')
        self.pin('private.explicit_ref_wins', doc,
                 'An explicit `PMAX_IMAGE_REF` always wins on unsigned and signed invocations: phase 50 uses that reference without a build or an age/source-commit check')
        self.absent('private.no_age_check_on_explicit', doc, 'remains subject to the same source and age checks')
        self.absent('private.no_launch_commitment', doc, 'are commitments at pass-2 launch')
        self.pin('private.generation_bound_completion', doc,
                 "an older generation's completion record does not complete a new generation using the same image.")
        self.pin('pass1.private_digest_source', record,
                 "from `deployment.yaml`'s `image:` field written by phase 65 or the "
                 "`PMAX_IMAGE_DIGEST=` value in phase 80's printed LOCAL line, for the signed pass.")
        metadata = self.scope(doc, '### Config save and observation recipes')
        self.pin('metadata.private_default', metadata,
                 'If a successful dataset metadata response omits `maxTimeTravelHours`, '
                 'record 168 hours with `time_travel_hours_source: default`; otherwise '
                 'record the value with `time_travel_hours_source: metadata`.')
        self.pin('metadata.private_failed_call', metadata,
                 'A failed dataset metadata call is NO-GO; a successful response '
                 'with the time-travel field absent uses the documented default:')
        self.pin('metadata.recipe_effective_hours', metadata,
                 'maxTimeTravelHours: ((.maxTimeTravelHours // "168") | tonumber)')
        self.pin('metadata.recipe_provenance', metadata,
                 'time_travel_hours_source: (if .maxTimeTravelHours == null then "default" else "metadata" end)')
        self.pin('evidence.scratch_outside', privacy, 'Raw address-bearing IAM, execution and audit-log outputs go only to a local scratch path outside the repository, then are deleted after their count/role or match/mismatch summary is written.')

    def test_after_invariants_and_real_run_reconciliation(self):
        doc = self.document('RUNBOOK.md', private=True)
        recon = self.scope(doc, '### Scheduled-run budget reconciliation')
        self.pin('budget.real_runs', recon, 'This recipe applies to real nightly runs only.')
        self.pin('budget.parent_sql', recon, 'Excluding child jobs makes the submission count comparable and avoids counting a script and its statements twice.')
        self.pin('budget.parent_sql.fragment', self.sql(recon, 'budget.sql'), 'AND parent_job_id IS NULL')
        self.pin('budget.run_id_sql', self.sql(recon, 'budget.sql'), "WHERE label.key = 'run_id' AND label.value = @run_id")
        self.pin('budget.scripts_once', recon,
                 "Scripts count once, matching the report's `(submissions)` line; do not add child jobs a second time.")
        weekly = self.scope(doc, '### Weekly credential-flip detector')
        self.pin('detector.cadence', weekly, 'every Monday after the scheduled run')
        self.pin('detector.allowed_auditor', weekly, 'An audit-labelled job is exempt only when its principal is the recorded operator account or runtime service account and it has both `app=pmax` and `stage=audit`.')
        self.pin('detector.weekly_exemption', weekly, 'The weekly check requires both audit labels and a recorded operator/runtime principal for any audit exemption; labels on another principal never exempt it.')
        self.pin('detector.spoofed_labels', weekly, 'Caller-set labels alone never authorize a reader; any other principal with those labels is an unexplained reader.')
    def test_public_after_invariants_and_evidence_recipes(self):
        doc = self.document('docs/migrations/v2.1.0.md')
        intro = self.scope(doc, '# Upgrade to v2.1.0', '## Prepare the guards before step 0')
        self.pin('evidence.never_commit_plan', intro,
                 '`--plan` prints configured principals, including editor addresses; never commit it.')
        self.pin('evidence.private_scratch_permissions', intro, 'umask 077')
        self.pin('evidence.unique_scratch', intro,
                 'EVIDENCE_SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/pmax-migration-evidence.XXXXXX")')
        self.pin('evidence.scratch_fail_closed', intro, ': "${EVIDENCE_SCRATCH:?')
        self.pin('evidence.scratch_strict_shell', intro, 'set -euo pipefail\numask 077')
        self.pin('evidence.scratch_cleanup', intro,
                 "trap 'rm -rf \"$EVIDENCE_SCRATCH\"' EXIT")
        self.pin('dates.alter_day', intro,
                 '`ALTER_DAY` is the UTC day of the proposed live retention ALTER;')
        self.pin('dates.published_as_of', intro,
                 "`AS_OF` is the published run's reporting date, read from its report; "
                 'use it for Q6 and the +24 h/+72 h reporting-window checks, not for loss acceptance.')
        recipes = self.scope(doc, '## Evidence query recipes', '**Q1, live partition loss and minimums.**')
        commands = [line for block in re.findall(r'```bash\n(.*?)\n```', recipes, re.S)
                    for line in block.replace('\\\n', ' ').splitlines()
                    if line.startswith('bq query ')]
        self.assertTrue(len(commands) == 1, 'Document pin evidence.runner: missing unique query runner')
        runner = commands[0]
        for label, option in (
            ('cap', '--maximum_bytes_billed="$EVIDENCE_BYTES_CAP"'),
            ('app', '--label=app:pmax'),
            ('env', '--label="env:$PMAX_ENV"'),
            ('audit', '--label=stage:audit'),
        ):
            self.pin('evidence.runner.' + label, runner, option)
        self.pin('dates.cutoff_alter_day', recipes,
                 'set `CUTOFF` to the ISO date computed from the pass-2 `ALTER_DAY` and window.')
        q1 = self.scope(doc, '**Q1, live partition loss and minimums.**', '**Q2')
        self.pin('dates.q1_cutoff_alter_day', q1, 'Bind `@cutoff` to\n`ALTER_DAY - reporting_window_days - 1`')
        after_dates = self.scope(doc, '## Go/No-Go and after-invariants')
        self.pin('dates.after_bound_published_as_of', after_dates, 'is at or after `AS_OF - reporting_window_days - 1` in window mode')
        self.pin('dates.q6_published_as_of', after_dates, "`AS_OF - R` through `AS_OF - 1`, using the published report's `AS_OF` in Q6")
        after = self.scope(doc, '## Go/No-Go and after-invariants')
        for name, needle in (
            ('all_null', 'Live option map all NULL for an anchor upgrade, eight legacy views present before 68, dataset defaults unset, and cutover query returns zero readers.'),
            ('iamcredentials', 'Looker account created with listed organization agents, all editors are users of those organizations, `iamcredentials` enabled, pre-check green.'),
            ('labels', 'Resource labels applied and parity evidence records them; rollback image and clean source checkout verified; snapshots refreshed on a different pass-2 day.'),
            ('preserve_pass1', 'Preserve the separate regular `observation-before-<digest>-<generation>-pass1.json` and `observation-before-<digest>-<generation>-pass2.json` in the Go/No-Go evidence directory; `observation-before.json` remains the regular active reading.'),
            ('both_triples', 'Record preserved pass 1, active pass 2 and the after reading from `observation-gate-<digest>.json`. The migration itself must leave the observation triple unchanged; explain manual/drill additions and require no component regression from the preserved pass-1 baseline.'),
            ('allowed_auditor', 'Audit labels exempt a job only when its principal is the recorded operator or runtime service account; any other labelled principal is unexplained.'),
        ):
            self.pin('after.' + name, after, needle)
        query = self.scope(doc, '**Q5, pre-drop cutover readers.**', '**Q6, reporting versus marts.**')
        self.pin('cutover.exclude_ddl', self.sql(query, 'cutover.sql'), "statement_type NOT IN ('CREATE_VIEW', 'DROP_VIEW')")
        self.pin('cutover.exclude_runtime', self.sql(query, 'cutover.sql'), 'user_email != @runtime_account')
        self.pin('cutover.audit_principal', self.sql(query, 'cutover.sql'), 'user_email IN (@operator_account, @runtime_account)')
        self.pin('cutover.covered_interval', query, 'covered interval')
        q6 = self.scope(doc, '**Q6, reporting versus marts.**', '## Rollback appendix')
        self.pin('reporting.reconciliation_recipe', q6,
                 'Run after publication through the labelled, capped runner above, as the recorded operator.')
        self.pin('reporting.all_eight', q6,
                 'Repeat the pattern for all eight reporting tables, taking the exact source, grouping, additive columns and cohort inclusion predicates from `src/pmax_pack/sql/reporting/publish_reporting.sql`;')
        self.pin('reporting.source_sql', self.sql(q6, 'reporting.sql'), 'FROM `PROJECT.MARTS.mart_campaign_truth`')
        self.pin('reporting.target_sql', self.sql(q6, 'reporting.sql'), 'FROM `PROJECT.REPORTING.campaign_truth`')
        self.pin('reporting.full_join', self.sql(q6, 'reporting.sql'), 'FROM expected AS e FULL OUTER JOIN actual AS a USING (date)')
        self.pin('reporting.absolute_tolerance', q6,
                 '--parameter="absolute_tolerance:FLOAT64:$ABSOLUTE_TOLERANCE"')
        self.pin('reporting.relative_tolerance', q6,
                 '--parameter="relative_tolerance:FLOAT64:$RELATIVE_TOLERANCE"')
        q6_sql = self.sql(q6, 'reporting.sql')
        for field in ('conversions', 'conversions_value', 'all_conversions',
                      'all_conversions_value'):
            self.pin('reporting.null_mismatch.' + field, q6_sql,
                     f'(e.{field} IS NULL) != (a.{field} IS NULL)')
            self.pin('reporting.tolerance.' + field, q6_sql,
                     f'ABS(e.{field} - a.{field}) > @absolute_tolerance + '
                     f'@relative_tolerance * GREATEST(ABS(e.{field}), ABS(a.{field}))')
            self.absent('reporting.no_exact_float.' + field, q6_sql,
                        f'e.{field} IS DISTINCT FROM a.{field}')
        self.pin('evidence.public_count_only', doc,
                 'Operator-written evidence records principals by count and role, never by address; the config generation identifies the editor list.')

    def test_paused_morning_duty_and_offboarding_order(self):
        doc = self.document('RUNBOOK.md', private=True)
        morning = self.scope(doc, '### Paused-morning duty')
        self.pin('morning.every_day', morning, 'On every paused scheduled morning from step 0 through phase-95 resume, check PAUSED and no live lease/incomplete execution, then run:')
        self.pin('morning.single_sitting', morning, "Step 0's config upload and pass 1 through phase 70 must happen in one sitting after that morning's scheduled run in the deployment timezone has completed.")
        self.pin('morning.saved_config', morning, 'If phase 70 cannot finish before the next morning, re-upload the saved pre-v2.1.0 config bytes from their recorded generation before that morning, record the restored generation, run the manual execution, then re-upload the new config and record that generation before resuming pass 1.')
        self.pin('morning.two_generations', morning, 'record the restored generation, run the manual execution, then re-upload the new config and record that generation before resuming pass 1.')
        offboard = self.scope(doc, '### Reader-account offboarding')
        self.ordered('offboarding.steps', offboard,
                     ('last seven days', 'Notify or repoint', 'Disable',
                      'token mint and a chart refresh fail',
                      'Remove the reporting dataset access entry and bindings', '30-day hold'))
        self.pin('offboarding.disabled_readback', offboard, "--format='value(disabled)'")
        self.pin('offboarding.disabled_reason', offboard, "The token-mint failure must name the disabled account in the private scratch result; a failure caused only by the operator's expired Token Creator grant is not proof.")
        incident = self.scope(doc, '### Incident checkpoint reset and deletion boundary')
        self.pin('checkpoint.wait_buffer', incident,
                 'Wait until `tables.get` shows no `streamingBuffer` on `load_checkpoints`, then re-run the same reset:')

    def test_public_operations_recovery_contract(self):
        doc = self.document('docs/operations.md')
        scopes = {
            'checkpoint': '## History checkpoints',
            'observe': '## Daily execution and failure handling',
            'publish': '## Rebuild and publish controls',
            'detector': '## Credential-flip detector',
            'incident': '## Incident deletion and recovery',
            'budget': '## Runtime budget and query cost',
        }
        for name, needle in (
            ('checkpoint.hash', 'The checkpoint hash contains only the family A, B and C query texts plus the Google Ads API version.'),
            ('checkpoint.start_independent', 'Neither an explicit start_date nor the omitted-start default enters the hash.'),
            ('observe.deployment_timezone', 'Deployments require timezone_override; the observe stage uses that timezone.'),
            ('checkpoint.backfill_lease', 'Backfill acquires the shared run lease mode, so it cannot overlap another holder of the pipeline lease.'),
            ('observe.deadline', 'Complete the observe stage before midnight in the deployment timezone.'),
            ('observe.selection', 'Its rows stay in the append-only log, and the pack selects the lexically greatest run ID among successful observe stages for that account and observed date.'),
            ('publish.guard', 'A real rebuild refuses to replace a newer reporting generation unless --allow-older-as-of is supplied.'),
            ('publish.dry_guard', 'In a dry run the older-as-of publish guard is recorded as not evaluated (dry run), so an older-date refusal appears only on a real run.'),
            ('detector.cadence', 'Run the credential-flip detector in the Go/No-Go after-invariants, every Monday after the scheduled run, and after editor or data-source changes.'),
            ('detector.identity', 'Audit labels exempt only the recorded operator or runtime principal with both app=pmax and stage=audit; labels on another principal never authorize it.'),
            ('incident.observations', 'Delete the approved observed_date partitions together with their Avro copies.'),
            ('incident.outside_window', 'Reset the approved account/month checkpoint, supervise backfill to re-land the chunk, then rebuild with --window-start covering those dates.'),
            ('incident.in_window', '| Click days inside the re-pull window | They cannot be durably removed while the account remains configured.'),
            ('incident.no_mid_ladder', 'Incident deletion is operator-owned and never runs mid-ladder.'),
            ('incident.case2_observation_rows', 'Delete the reviewed range from every click-day table and copy, including observation rows for those click dates.'),
            ('incident.case_pair', 'Always perform the observation/Avro case for the affected history.'),
            ('incident.re_record', 'After deletion, re-record the active observation-before.json using the phase-25 query against the raw dataset and record the deletion and new triple in the incident record.'),
            ('incident.no_erasure', 'Do not claim immediate physical erasure.'),
            ('incident.retention', 'After a deletion, BigQuery keeps the data recoverable for the effective time-travel window, from two to seven days, followed by seven more days of fail-safe retention.'),
            ('checkpoint.buffer', 'If load_checkpoints has a streaming buffer, reset refuses before deleting anything.'),
            ('budget.parents', 'Excluding child jobs makes the submission count comparable and avoids counting a script and its statements twice.'),
        ):
            heading = '## Incident deletion and recovery' if name == 'checkpoint.buffer' else scopes[name.split('.')[0]]
            self.pin('operations.' + name, self.scope(doc, heading).replace('`', ''), needle)
        self.pin('operations.budget.parents.sql', self.sql(self.scope(doc, scopes['budget']), 'operations.budget.sql'), 'AND parent_job_id IS NULL')
        protection = self.scope(doc, '## Identities, credentials and data protection')
        for name, needle in (
            ('fields', 'The pack extracts aggregate advertising performance and entity configuration, including account and advertising-entity IDs; it does not extract end-user identifiers.'),
            ('principals', 'The permitted data principals are the runtime service account, the named operator, and the dedicated Looker service account on reporting only.'),
            ('sharing', "Share reports with named users or the client's domain, never link sharing."),
            ('audit', "Data Access audit logs identify the principal for successful reads and remain available for the log bucket's configured retention period, normally 30 days for the default bucket."),
            ('expiry', 'Automatic partition expiry produces no `TableDataChange` entry in this per-principal trail; inspect partition metadata to prove retention.'),
            ('avro', "Observation Avro copies under `observations/<account>/<observed_date>/` are outside the report bucket's lifecycle."),
            ('quota', 'The per-user daily query quota bounds Looker queries only under on-demand pricing.'),
        ):
            self.pin('operations.protection.' + name, protection, needle)
        offboarding = self.scope(doc, '### Reader-account offboarding')
        self.pin('operations.protection.revocation', offboarding, 'Remove the reporting dataset access entry and bindings: every listed service-agent Token Creator binding, every operator probe binding including stale conditions, and every editor Service Account User binding.')
        self.absent('operations.retired_hash', doc, 'default-90d')
        self.absent('operations.retired_dry_source', doc, 'dry_run_config_fallback')

    def retention_provenance(self, name, private=False):
        doc = self.document(name, private=private)
        self.pin('retention.recorded_provenance.' + name, doc,
                 'The retention record carries `time_travel_hours` and '
                 '`time_travel_hours_source` for raw and marts, including each attempt.')
        self.pin('retention.machine_identity.' + name, doc,
                 'Before invoking retention, the ladder verifies the ADC user principal '
                 'against the CLI account through tokeninfo and refuses a mismatch '
                 'or an unavailable verified principal.')

    def test_public_retention_record_and_operator_check_provenance(self):
        self.retention_provenance('docs/migrations/v2.1.0.md')

    def test_private_retention_record_and_operator_check_provenance(self):
        self.retention_provenance('RUNBOOK.md', private=True)

    def incident_baseline(self, name, heading, private=False):
        doc = self.scope(self.document(name, private=private), heading)
        for rule, needle in (
            ('preserve', 'An incident never edits or lowers a preserved per-generation pass baseline.'),
            ('abandon', "If an unfinished generation's preserved pass-1 baseline predates the deletion, abandon that generation and never resume it."),
            ('fresh', 'A fresh generation may reuse the same reviewed image digest; phase 25 captures its own generation-bound pass-1 baseline and the phase-95 observation gate reads that path from the validated continuation record.'),
            ('build', '`PMAX_FORCE_BUILD=1` with `PMAX_IMAGE_REF` unset starts a build and a generation; it does not guarantee a new digest.'),
            ('collision', 'If the same generation already has a conflicting preserved pass baseline, stop; never delete or lower prior evidence, including baselines from earlier generations.'),
        ):
            self.pin('incident.baseline.' + rule + '.' + name, doc, needle)

    def test_public_incident_deletion_starts_a_fresh_baseline(self):
        self.incident_baseline('docs/operations.md', '## Incident deletion and recovery')
        self.incident_baseline('docs/migrations/v2.1.0.md', '### Guard: resume')

    def test_private_incident_deletion_starts_a_fresh_baseline(self):
        self.incident_baseline('RUNBOOK.md', '### Incident checkpoint reset and deletion boundary', private=True)

    def detector_positive_controls(self, name, heading, private=False):
        doc = self.scope(self.document(name, private=private), heading)
        for rule, needle in (
            ('logging', 'Verify BigQuery DATA_READ audit logging is enabled for the project with no exemptedMembers.'),
            ('sink', 'Verify the log sink delivers these reads to the bucket queried.'),
            ('positive', 'Require at least one positive matched Looker or runtime read in the interval.'),
            ('inconclusive', 'Zero unexplained readers without that positive control is inconclusive, not a pass.'),
        ):
            self.pin('detector.control.' + rule + '.' + name, doc.replace('`', ''), needle)

    def test_public_detector_positive_controls(self):
        self.detector_positive_controls('docs/operations.md', '## Credential-flip detector')
        self.detector_positive_controls('docs/migrations/v2.1.0.md', '## Go/No-Go and after-invariants')

    def test_private_detector_positive_controls(self):
        self.detector_positive_controls('RUNBOOK.md', '### Weekly credential-flip detector', private=True)

    def test_public_operations_editor_credential_fallback(self):
        doc = self.document('docs/operations.md')
        self.pin('operations.editor.credential_fallback',
                 self.scope(doc, '## Identities, credentials and data protection'),
                 "Editing a service-account data source without `actAs` switches it to the editor's personal credentials.")

    def test_public_looker_editor_credential_fallback(self):
        doc = self.document('docs/looker.md')
        self.pin('looker.editor.credential_fallback',
                 self.scope(doc, '## Share, hand over, and revoke'),
                 "Editing a service-account data source without `actAs` switches it to the editor's personal credentials.")

    def test_public_bootstrap_inputs(self):
        doc = self.document('deploy/iam.md')
        section = self.scope(doc, '## Bootstrap inputs')
        block = re.search(r'```bash\n(.*?)\n```', section, re.S)
        self.assertIsNotNone(block, 'bootstrap shell block')
        for variable in ('PMAX_DEPLOYER_MEMBER', 'PMAX_OPERATOR_MEMBER',
                         'PMAX_GITHUB_REPOSITORY_ID', 'PMAX_GITHUB_OWNER_ID',
                         'PMAX_OAUTH_PUBLISHING_STATUS'):
            self.pin('bootstrap.' + variable, block.group(1), 'export ' + variable + '=')

    def test_public_quota_billed_byte_basis(self):
        doc = self.document('deploy/iam.md')
        quota = self.scope(doc, '### Measure and prove the query quota')
        self.pin('quota.billed_basis', quota,
                 'Size `PMAX_CI_DAILY_QUERY_QUOTA_MIB` from billed bytes; processed bytes are diagnostic only.')
        self.pin('quota.rate_normalization', quota,
                 'For higher-rate operations, reconcile the rate-normalized usage in Quotas & System Limits before choosing the allowance.')
        self.pin('quota.billed_sql', self.sql(quota, 'quota.sql'),
                 'CAST(CEIL(SUM(total_bytes_billed) / 1048576.0) AS INT64) AS measured_mib')

    def test_public_looker_sharing_and_asset_counting(self):
        doc = self.document('docs/looker.md')
        self.pin('looker.sharing', self.scope(doc, '## Share, hand over, and revoke'),
                 "Share reports with named users or the client's domain, never link sharing.")
        self.pin('looker.cohort_asset.counting', self.scope(doc, '### `pmax_reporting.cohort_asset`'),
                 "Counting filter: `cohort_counting = 'arp_calendar'`; apply the cohort filters")


if __name__ == '__main__':
    unittest.main(argv=['u17'], verbosity=2)
PY_U17
[[ "${PMAX_DOCS_ONLY:-0}" != 1 && "${U17_DOCS_ONLY:-0}" != 1 ]] || exit 0

uv run python - "$ROOT" <<'PY_ALERT_IMAGE'
from pathlib import Path
import os
import re
import subprocess
import sys
import unittest

root = Path(sys.argv[1])
source = (root / 'deploy/deploy.sh').read_text()
functions = '\n'.join(re.findall(r'^\w+\(\) \{\n.*?^\}', source, re.M | re.S))


class ImmutableAlertImageTests(unittest.TestCase):
    def test_alert_proof_refuses_tag_only_images(self):
        for image in ('test-image:latest', 'registry.example.test/test/image:latest'):
            with self.subTest(image=image):
                result = subprocess.run(
                    ['bash', '-c', 'set -euo pipefail\n' + functions + '\nalert_proof_path'],
                    env={**os.environ, 'PMAX_IMAGE_REF': image, 'ROOT': str(root),
                         'PROJECT': 'test-pmax-project'},
                    text=True, capture_output=True, stdin=subprocess.DEVNULL)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('alert-proof requires an immutable image digest',
                              result.stdout + result.stderr)
                self.assertNotIn('alert-proof-', result.stdout)


if __name__ == '__main__':
    unittest.main(argv=['immutable-alert-image'], verbosity=2)
PY_ALERT_IMAGE

# Upgrade fixtures select storage; the missing-storage case removes it explicitly.
printf '\nstorage: window\n' >>"$TMP/config.yaml"
uv run python - "$TMP/bin/gcloud" <<'PY_U11_LEGACY_CAT'
from pathlib import Path
import sys
path = Path(sys.argv[1])
source = path.read_text()
source = source.replace('case "$*" in', 'case "$*" in\n  "storage cat "*) cat "$FAKE_CONFIG" ;;', 1)
path.write_text(source)
PY_U11_LEGACY_CAT

# Actual phase contracts with isolated cloud metadata and CLI leaves.
if [[ "${U18_TESTS_ONLY:-0}" != 1 ]]; then
uv run python - "$ROOT" "$TMP" <<'PY_U11'
from __future__ import annotations
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import yaml
from pmax_pack.config import load_config

root, base = map(Path, sys.argv[1:])


class MigrationRetentionDeployTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.directory.cleanup)
        self.work = Path(self.directory.name)
        self.bin = self.work / 'bin'
        self.bin.mkdir()
        self.log = self.work / 'calls.jsonl'
        self.signed = self.work / 'signed.yaml'
        self.signed.write_text('decision: approved\n')
        self.config = self.work / 'config.yaml'
        self.config.write_text((base / 'config.yaml').read_text() + '\nreporting_window_days: 90\nstart_date: 2026-06-01\n')
        self.env = {
            **os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}',
            'U11_REAL_UV': shutil.which('uv'), 'U11_LOG': str(self.log),
            'U11_CONFIG': str(self.config), 'U11_SOURCE_ROOT': str(root),
            'U11_REMOTE_CONFIG': str(self.work / 'remote-config.yaml'),
            'ROOT': str(self.work), 'PHASE_ROOT': str(root / 'deploy/phases'),
            'PLAN': '0', 'ASSUME_YES': '0',
            'PROJECT': 'test-pmax-project', 'REGION': 'europe-west1',
            'WORK_DIR': str(self.work / 'evidence'), 'CONFIG_LOCAL': str(self.config),
            'CONFIG_FILE': str(self.config), 'CONFIG_URI': 'gs://test-config-bucket/test.yaml',
            'PHASE_STATE': str(self.work / 'phase.env'), 'UPGRADE': '1',
            'CREDENTIAL_FILE': str(base / 'credential.yaml'),
            'OPERATOR_IDENTITY': 'operator@example.test', 'SECRET_NAME': 'pmax-google-ads',
            'SECRET_VERSION': '7', 'OAUTH_STATUS': 'production',
            'REPORT_BUCKET': 'test-report-bucket', 'CONFIG_BUCKET': 'test-config-bucket',
            'IMAGE_REF': 'europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current', 'RUN_DAY': '2026-09-18',
            'PMAX_MIGRATION_REVIEWED': '1', 'PMAX_RETENTION_CONFIRMED': '91',
            'PMAX_CONFIRMED_PHASES': '68-migration,89-retention',
            'PMAX_SIGNED_REVIEW': '', 'PMAX_IMAGE_REF': '',
            'PMAX_EXECUTION_MAX_POLLS': '1', 'PMAX_EXECUTION_POLL_SECONDS': '0',
            'RUNTIME_SA': 'pmax-runtime@test-pmax-project.iam.gserviceaccount.com',
            'DATASET_RAW': 'pmax_raw', 'DATASET_MARTS': 'pmax_marts',
            'DATASET_OPS': 'pmax_ops', 'DATASET_VERIFY': 'pmax_marts_verify',
            'DATASET_REPORTING': 'pmax_reporting', 'DATASET_REPORTING_VERIFY': 'pmax_reporting_verify',
            'DATASET_SNAPSHOTS': 'pmax_snapshots',
            'STORAGE': 'window', 'REPORTING_WINDOW_DAYS': '90', 'REVIEW_RECORDED': '1',
            'PMAX_ANCHOR_CHECKOUT': str(root), 'PMAX_ANCHOR_CONFIG': str(self.config),
        }
        self.env.pop('LADDER_GENERATION', None)
        Path(self.env['WORK_DIR']).mkdir()
        (self.work / 'deploy').symlink_to(root / 'deploy', target_is_directory=True)
        (self.work / 'src').symlink_to(root / 'src', target_is_directory=True)
        self.adc = self.work / 'adc.json'
        self.adc.write_text(json.dumps({'type': 'authorized_user'}))
        self.env['GOOGLE_APPLICATION_CREDENTIALS'] = str(self.adc)
        self.state = self.work / 'options.json'
        self.env['U11_STATE'] = str(self.state)
        self.seed_options(differing=False)
        self.functions = self.work / 'functions.sh'
        source = (root / 'deploy/deploy.sh').read_text()
        self.functions.write_text('\n'.join(re.findall(r'^\w+\(\) \{\n.*?^\}', source, re.M | re.S)))
        shim = r'''
import json, os, shutil, sys
from pathlib import Path
args = sys.argv[1:]
command = Path(sys.argv[0]).name
with open(os.environ['U11_LOG'], 'a') as f:
    f.write(json.dumps({'command': command, 'args': args, 'impersonation': os.environ.get('CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT', '')}) + '\n')
text = ' '.join(args)
fault = os.environ.get('U11_FAULT', '')
if command == 'uv':
    if 'pmax-pack' in args:
        command_args = args[args.index('pmax-pack') + 1:]
        if command_args[0] != 'retention': sys.exit(0)
        sys.path.insert(0, os.environ['U11_SOURCE_ROOT'] + '/src')
        import re
        from types import SimpleNamespace
        from unittest.mock import patch
        from pmax_pack import cli, retention
        from pmax_pack.config import load_config
        config = load_config(os.environ.get('PMAX_CONFIG', os.environ['U11_CONFIG']))
        state_path = Path(os.environ['U11_STATE'])
        def query(client, cfg, run_id, sql, params=None):
            with open(os.environ['U11_LOG'], 'a') as f:
                f.write(json.dumps({'command': 'retention_sql', 'args': [sql]}) + '\n')
            options = json.loads(state_path.read_text())
            if 'TABLE_OPTIONS' in sql:
                dataset = re.search(r'FROM `([^`]+)\.INFORMATION_SCHEMA', sql).group(1)
                return [{'table_name': table.rsplit('.', 1)[-1], 'option_name': 'partition_expiration_days',
                         'option_value': value} for table, value in options.items()
                        if table.rsplit('.', 1)[0] == dataset]
            if 'SCHEMATA_OPTIONS' in sql:
                return [{'schema_name': name, 'option_name': 'max_time_travel_hours', 'option_value': hours}
                        for name, hours in [('pmax_raw', 48), ('pmax_marts', 72)]]
            if sql.startswith('ALTER TABLE'):
                table, value = re.search(r'ALTER TABLE `([^`]+)` SET OPTIONS \(partition_expiration_days = (NULL|\d+)\)', sql).groups()
                options[table] = None if value == 'NULL' else int(value)
                state_path.write_text(json.dumps(options))
                return []
            raise AssertionError('unexpected retention SQL: ' + sql)
        with patch.object(cli, '_load_runtime_dependencies', return_value=SimpleNamespace(config=config, bq_client=object())), patch.object(retention, '_query', side_effect=query):
            sys.exit(cli.main(command_args))
    if args[:3] == ['run', 'python', '-']:
        import subprocess
        code = sys.stdin.read()
        if '# RETENTION_ADC_PRINCIPAL' in code:
            prelude = """
import google.auth
from google.oauth2.credentials import Credentials
import requests
import os, json
from pathlib import Path
from unittest.mock import Mock
fault = os.environ.get("U11_FAULT", "")
google.auth.default = lambda **kw: (Credentials("TEST_ADC_TOKEN_NEVER_PRINT"), None)
Credentials.refresh = lambda self, request: None
def tokeninfo(url, **kwargs):
    assert url == "https://oauth2.googleapis.com/tokeninfo"
    assert kwargs["params"] == [("access_token", "TEST_ADC_TOKEN_NEVER_PRINT")]
    assert 0 < kwargs["timeout"] <= 30
    with open(os.environ["U11_LOG"], "a") as stream:
        stream.write(json.dumps({"command":"adc_tokeninfo", "args":[]}) + "\\n")
    if fault == "adc_network":
        raise requests.RequestException("TEST_ADC_TOKEN_NEVER_PRINT")
    data = {"email": "different@example.test" if fault == "adc_mismatch" else "operator@example.test", "email_verified": "true"}
    if fault == "adc_missing_email": data.pop("email")
    if fault == "adc_unverified_email": data["email_verified"] = "false"
    if fault == "adc_service_email": data["email"] = "runtime@fixture.iam.gserviceaccount.com"
    return Mock(json=lambda: data, raise_for_status=lambda: None)
requests.get = tokeninfo
"""
            code = prelude + '\nexec(compile(' + repr(code) + ', "<adc-check>", "exec"))'
        if fault == 'submission_write' and '".submission-"' in code:
            prelude = """
from pathlib import Path
original_open = Path.open
def fail_submission_write(path, *args, **kwargs):
    if path.name.startswith(".submission-"):
        raise OSError("injected submission staging failure")
    return original_open(path, *args, **kwargs)
Path.open = fail_submission_write
"""
            code = prelude + '\nexec(compile(' + repr(code) + ', "<submission-write>", "exec"))'
        sys.exit(subprocess.run([os.environ['U11_REAL_UV'], *args], input=code, text=True).returncode)
    os.execv(os.environ['U11_REAL_UV'], ['uv', *args])
if command == 'git':
    if len(args) == 4 and args[0] == '-C' and args[2:] == ['rev-parse', 'HEAD']:
        print(('c' if fault == 'wrong_anchor' else 'b' if fault == 'public_anchor' else 'a') * 40); sys.exit(0)
    if args[2:] == ['status', '--porcelain', '--untracked-files=all']:
        paths = {'anchor_tracked': ' M README.md',
                 'anchor_untracked_src': '?? src/pmax_pack/injected.py',
                 'anchor_untracked_deploy': '?? deploy/injected.sh',
                 'anchor_untracked_root': '?? unexpected.txt'}
        if fault in paths: print(paths[fault])
        elif fault == 'anchor_status_error': sys.exit(3)
        sys.exit(0)
    sys.exit(2)
if command == 'gcloud':
    if args[:3] == ['config', 'get-value', 'account']:
        print('runtime@fixture.iam.gserviceaccount.com' if fault == 'adc_service_email' else os.environ['RUNTIME_SA'] if fault == 'runtime_operator' else ('' if fault == 'empty_operator' else 'operator@example.test'))
    elif args[:4] == ['alpha', 'monitoring', 'policies', 'list']: print('test-policy')
    elif args[:4] == ['alpha', 'monitoring', 'policies', 'update']: pass
    elif args[:3] == ['run', 'jobs', 'execute']:
        if fault == 'alert_submit_error': sys.exit(1)
        print('failed-probe-execution')
    elif args[:4] == ['run', 'jobs', 'executions', 'describe']:
        if fault == 'alert_describe_error': sys.exit(1)
        if fault == 'alert_running': print('\t0\t0')
        elif fault == 'alert_no_failed_tasks': print('2026-09-18T01:00:00Z\t0\t0')
        elif fault == 'alert_succeeded': print('2026-09-18T01:00:00Z\t1\t0')
        else: print('2026-09-18T01:00:00Z\t\t1')
    elif args[:3] in (['scheduler', 'jobs', 'resume'], ['scheduler', 'jobs', 'pause']): pass
    elif args[:4] == ['artifacts', 'docker', 'images', 'describe']: print('sha256:current')
    elif args[:2] == ['projects', 'describe']:
        print('1' if 'projectNumber' in text else 'pmax')
    elif args[:2] == ['projects', 'get-ancestors']: print('project\norganization')
    elif args[:3] == ['billing', 'projects', 'describe']: print('True')
    elif args[:2] == ['auth', 'list']: print('operator@example.test')
    elif args[:3] == ['resource-manager', 'org-policies', 'describe']:
        print('{"spec":{"rules":[{"allowAll":true,"enforce":true}]}}')
    elif args[:3] == ['run', 'jobs', 'describe']:
        if os.environ.get('UPGRADE') == '0': sys.exit(1)
        print(os.environ['IMAGE_REF'] if 'containers[0].image' in text else 'pmax-pack-daily')
    elif args[:2] == ['storage', 'cp']:
        remote = Path(os.environ['U11_REMOTE_CONFIG'])
        if args[2].startswith('gs://'):
            shutil.copyfile(remote if remote.exists() else os.environ['U11_CONFIG'], args[3])
        else:
            shutil.copyfile(args[2], remote)
    elif args[:3] == ['scheduler', 'jobs', 'describe']: print('ENABLED' if fault == 'scheduler' else 'PAUSED')
    elif args[:3] == ['storage', 'objects', 'describe']:
        if args[3].endswith('/lease.json'):
            if fault in ('lease', 'expired_lease', 'lease_malformed', 'lease_execution'): print('{"generation":"7"}')
            elif fault == 'lease_unreadable': print('permission denied', file=sys.stderr); sys.exit(1)
            else: print('ERROR: 404 NotFoundException', file=sys.stderr); sys.exit(1)
        else:
            if fault == 'config_generation_drift':
                calls = [json.loads(line) for line in Path(os.environ['U11_LOG']).read_text().splitlines()]
                reads = sum(call['command'] == 'gcloud' and call['args'][:3] == ['storage', 'objects', 'describe']
                            and not call['args'][3].endswith('/lease.json') for call in calls)
                print(json.dumps({'name': 'test.yaml', 'generation': '7' if reads == 1 else '8'}))
            else: print(os.environ.get('U11_GENERATION_JSON', '{"name":"test.yaml","generation":"7"}'))
    elif args[:2] == ['storage', 'cat']:
        if args[2] == os.environ['CONFIG_URI']:
            remote = Path(os.environ['U11_REMOTE_CONFIG'])
            content = (remote if remote.exists() else Path(os.environ['U11_CONFIG'])).read_bytes()
            if fault == 'config_drift':
                content = content.replace(b'reporting_window_days: 90', b'reporting_window_days: 120')
            sys.stdout.buffer.write(content)
            sys.exit(0)
        if fault == 'lease_malformed': print('{}'); sys.exit(0)
        print(json.dumps({'expires_at': '2099-01-01T00:00:00+00:00' if fault in ('lease', 'lease_execution') else '2000-01-01T00:00:00+00:00'}))
    elif args[:4] == ['run', 'jobs', 'executions', 'list']:
        if os.environ.get('U11_EXECUTIONS_JSON'):
            print(os.environ['U11_EXECUTIONS_JSON']); sys.exit(0)
        if fault == 'large_execution_list':
            print(json.dumps([{'metadata': {'name': f'execution-{i}', 'annotations': {'padding': 'x' * 1024}},
                              'status': {'completionTime': '2026-09-18T00:00:00Z', 'succeededCount': 1}}
                             for i in range(1200)])); sys.exit(0)
        if fault == 'execution_malformed': print('{}'); sys.exit(0)
        if fault == 'execution_null': print('[null]'); sys.exit(0)
        if fault == 'execution_failed': print('[{"status":{"completionTime":"2026-09-18T00:00:00Z","failedCount":1}}]'); sys.exit(0)
        if fault in ('execution', 'lease_execution'): print('[{"metadata":{"name":"running"},"status":{"succeededCount":1}}]')
        else: print('[{"metadata":{"name":"done"},"status":{"completionTime":"2026-09-18T00:00:00Z","succeededCount":1}}]')
    else:
        print('unsupported gcloud leaf: ' + text, file=sys.stderr); sys.exit(2)
    sys.exit(0)
if command == 'bq':
    if 'query' not in args: sys.exit(2)
    if 'SELECT run_id, status' in text:
        print(json.dumps([{'run_id': 'ldo-fixture', 'status': 'SUCCESS'}, {'run_id': 'ldc-fixture', 'status': 'SUCCESS' if fault == 'skipped_status' else 'SKIPPED'}]))
    elif 'raw_observations' in text:
        print(json.dumps([{'row_count': '12', 'observed_days': '3', 'latest_observed_day': '2026-09-18'}]))
    else: print('[]')
    sys.exit(0)
print('unexpected command: ' + command, file=sys.stderr)
sys.exit(2)
'''
        for command in ('gcloud', 'bq', 'uv', 'docker', 'git'):
            path = self.bin / command
            path.write_text(f'#!{sys.executable}\n' + shim)
            path.chmod(0o755)

    def run_phase(self, phase, extra='', **values):
        # Only standalone post-phase-25 alert/resume cases supply a generation.
        defaults = {'LADDER_GENERATION': 'f'*32} if phase in ('90-alert.sh', '95-resume.sh') else {}
        env = {**self.env, **defaults, **values}
        # Each isolated invocation models a driver run with its own scratch directory.
        Path(env['WORK_DIR']).mkdir(exist_ok=True)
        return subprocess.run(['bash', '-c', 'set -euo pipefail; source "$1"; source "$2"\n' + extra,
                               'bash', str(self.functions), str(root / 'deploy/phases' / phase)],
                              env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              stdin=subprocess.DEVNULL)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_human_phase_markers_and_typed_values_are_not_authorization(self):
        source = (root / 'deploy/deploy.sh').read_text()
        for phase in ('68-migration', '89-retention'):
            with self.subTest(phase=phase):
                self.assertIn(f'"{phase}|human-run"', source)
                result = subprocess.run(['bash', '-c', 'source "$1"; confirm_human_phase "$2"',
                                         'bash', str(self.functions), phase],
                                        env={**self.env, 'PMAX_CONFIRMED_PHASES': ''},
                                        stdin=subprocess.DEVNULL, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('needs the operator', result.stderr)

    def test_signed_pass_requires_image_before_any_cloud_call(self):
        result = subprocess.run(['bash', str(root / 'deploy/deploy.sh'), '--project', self.env['PROJECT'],
                                 '--region', self.env['REGION'], '--config-uri', self.env['CONFIG_URI'],
                                 '--credential-file', self.env['CREDENTIAL_FILE'], '--plan'],
                                env={**self.env, 'PMAX_SIGNED_REVIEW': 'signed.json'},
                                stdin=subprocess.DEVNULL, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('PMAX_IMAGE_REF', result.stdout + result.stderr)
        self.assertEqual(self.calls(), [])

    def test_upgrade_without_storage_refuses_by_key(self):
        data = yaml.safe_load(self.config.read_text())
        data.pop('storage')
        self.config.write_text(yaml.safe_dump(data))
        result = self.run_phase('00-preflight.sh', PLAN='1', CONFIG_FILE='',
                                CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('storage', result.stdout)

    def test_fresh_without_storage_defaults_window(self):
        data = yaml.safe_load(self.config.read_text())
        data.pop('storage')
        self.config.write_text(yaml.safe_dump(data))
        result = self.run_phase('00-preflight.sh', PLAN='1', UPGRADE='0',
                                CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('window', Path(self.env['PHASE_STATE']).read_text())

    def test_phase68_plan_prints_migration_before_live_marts_dry_run(self):
        result = self.run_phase('68-migration.sh', PLAN='1', PMAX_MIGRATION_REVIEWED='0')
        self.assertEqual(result.returncode, 0, result.stdout)
        text = result.stdout.replace('\\', '')
        self.assertEqual(text.count('DROP VIEW IF EXISTS'), 8)
        self.assertEqual(text.count('ADD COLUMN IF NOT EXISTS cohort_counting'), 6)
        self.assertIn('--target-dataset pmax_marts --dry-run', text)
        self.assertGreater(text.index('--dry-run'), text.rindex('CREATE TABLE'))
        self.assertFalse(any(c['command'] in ('gcloud', 'bq') or
                             (c['command'] == 'uv' and 'pmax-pack' in c['args']) for c in self.calls()))


    def set_storage(self, mode):
        data = yaml.safe_load(self.config.read_text())
        data['storage'] = mode
        self.config.write_text(yaml.safe_dump(data))
        self.env['STORAGE'] = mode
        self.env['PMAX_RETENTION_CONFIRMED'] = 'never' if mode == 'incremental' else '91'

    def rehearsal(self, **updates):
        path = self.work / 'deployments' / self.env['PROJECT'] / 'rehearsal-evidence-sha256-current.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            'image_digest': self.env['IMAGE_REF'], 'rehearsal_passed': True,
            'option_map_asserted': True, 'never_expire_asserted': True,
            'cohort_counting_restored': True, 'anchor_scripts_passed': True,
            'anchor_rehearsal_applicable': True, 'anchor_source_commit': 'a' * 40,
            'anchor_digest': 'test-image@sha256:anchor', 'anchor_source_kind': 'private',
            'looker_probes_passed': True, 'confirmed_value': self.env['PMAX_RETENTION_CONFIRMED'],
            'target_dataset': 'pmax_marts_verify', **updates,
        }))
        return path

    def seed_options(self, differing=True, protected=False):
        from pmax_pack import cli, retention
        from pmax_pack.runner import load_manifest
        config = load_config(str(self.config))
        options = retention.expected_retention(config, load_manifest(cli._MANIFEST_PATH))
        click = [table for table, value in retention.expected_retention(
            __import__('dataclasses').replace(config, storage='window'),
            load_manifest(cli._MANIFEST_PATH)).items() if value is not None]
        targets = [next(table for table in click if '.pmax_marts.' in table),
                   next(table for table in click if '.pmax_raw.' in table)]
        if differing:
            for table in targets:
                options[table] = 30
        if protected:
            options['test-pmax-project.pmax_raw.raw_observations'] = 7
        self.state.write_text(json.dumps(options))
        return targets

    def test_pass2_preflight_refuses_each_input_before_later_phases(self):
        self.anchor_record()
        self.alert_proof()
        self.assertNotIn('LADDER_GENERATION', self.env)
        valid = {'PLAN': '1', 'CONFIG_FILE': '', 'CONFIG_LOCAL': str(self.work / 'validated.yaml'),
                 'PMAX_CONFIRMED_PHASES':'68-migration,89-retention,95-resume',
                 'PMAX_SIGNED_REVIEW': str(self.signed), 'PMAX_ALERT_CONFIRMED': '1',
                 'PMAX_SKIPPED_ALERT_SILENT': '1', 'PMAX_NOTIFICATION_CHANNEL': 'test-channel'}
        for key, value in [('PMAX_RETENTION_CONFIRMED', '90'), ('PMAX_ALERT_CONFIRMED', ''),
                           ('PMAX_SKIPPED_ALERT_SILENT', '')]:
            with self.subTest(key=key):
                result = self.run_phase('00-preflight.sh', **{**valid, key: value})
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('signed pass requires ' + ('95-resume' if key == 'PMAX_CONFIRMED_PHASES' else key), result.stdout)
        result = self.run_phase('00-preflight.sh', **valid)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.set_storage('incremental')
        result = self.run_phase('00-preflight.sh', **valid)
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_config_record_preserves_object_generation(self):
        result = self.run_phase('65-config.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        path = self.work / 'deployments' / self.env['PROJECT'] / 'deployment.yaml'
        self.assertEqual(str(yaml.safe_load(path.read_text())['config_generation']), '7')

    def test_phase68_refusals_precede_ddl(self):
        cases = [('scheduler', {}, 'PAUSED'), ('', {'PMAX_MIGRATION_REVIEWED': '0'}, 'PMAX_MIGRATION_REVIEWED'),
                 ('lease', {}, 'lease'), ('execution', {}, 'execution'), ('lease_unreadable', {}, 'lease')]
        for fault, values, needle in cases:
            with self.subTest(fault=fault, values=values):
                self.log.write_text('')
                result = self.run_phase('68-migration.sh', U11_FAULT=fault, **values)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(needle, result.stdout)
                self.assertFalse(any(c['command'] == 'bq' for c in self.calls()), self.calls())
                self.assertFalse(any(c['command'] == 'retention_sql' for c in self.calls()))
        # Scheduler and review are checked before the lease or execution list.
        self.log.write_text('')
        self.run_phase('68-migration.sh', U11_FAULT='scheduler', PMAX_MIGRATION_REVIEWED='0')
        self.assertFalse(any(c['args'][:3] == ['storage', 'objects', 'describe'] for c in self.calls()))

    def test_post68_dry_run_covers_every_cohort_and_publish_insert(self):
        result = self.run_phase('68-migration.sh', U11_FAULT='expired_lease')
        self.assertEqual(result.returncode, 0, result.stdout)
        queries = [c['args'] for c in self.calls() if c['command'] == 'bq']
        live = [args[-1] for args in queries if not any(a.startswith('--dry_run') for a in args)]
        dry = [args[-1] for args in queries if any(a.startswith('--dry_run') for a in args)]
        self.assertEqual(sum('DROP VIEW IF EXISTS' in sql for sql in live), 8)
        self.assertEqual(sum('ADD COLUMN IF NOT EXISTS cohort_counting' in sql for sql in live), 6)
        self.assertEqual(sum('CREATE TABLE' in sql for sql in live), 8)
        first_dry = next(i for i, args in enumerate(queries) if any(a.startswith('--dry_run') for a in args))
        self.assertEqual(first_dry, 22)
        self.assertTrue(all(any(a.startswith('--maximum_bytes_billed=') for a in args) for args in queries))
        self.assertFalse(any('CREATE TEMP TABLE' in sql for sql in dry))
        cohorts = set()
        reporting = set()
        for sql in dry:
            for dataset, table in re.findall(r'INSERT\s+INTO\s+`[^`.]+\.([^`.]+)\.([^`]+)`', sql, re.I):
                if table in {'int_lag_prefix_campaign', 'int_lag_prefix_asset_group', 'int_observation_cells',
                             'mart_cohort_campaign', 'mart_cohort_asset_group', 'mart_cohort_asset'} and dataset == 'pmax_marts':
                    cohorts.add(table)
                if dataset == 'pmax_reporting': reporting.add(table)
        self.assertEqual(len(cohorts), 6, cohorts)
        self.assertEqual(len(reporting), 8, reporting)
        from pmax_pack import cli
        from pmax_pack.runner import load_manifest
        for step in load_manifest(cli._MANIFEST_PATH).steps:
            self.assertIn(f'VALIDATE  {step.name} statement ', result.stdout)
        cli_calls = [c for c in self.calls() if c['command'] == 'uv' and 'pmax-pack' in c['args']]
        self.assertTrue(any('rebuild' in c['args'] and '--dry-run' in c['args'] for c in cli_calls))

    def test_phase68_fixed_retired_view_inventory(self):
        result = self.run_phase('68-migration.sh', PLAN='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        rendered = result.stdout.replace('\\', '')
        expected = {'v_asset_performance', 'v_campaign_truth', 'v_cohort_asset', 'v_cohort_asset_group',
                    'v_cohort_campaign', 'v_performance_asset', 'v_performance_asset_group', 'v_performance_campaign'}
        self.assertEqual(set(re.findall(r'DROP VIEW IF EXISTS `[^`.]+\.[^`.]+\.([^`]+)`', rendered)), expected)

    def test_phase68_incremental_clears_differing_options_before_relanding(self):
        self.set_storage('incremental')
        targets = self.seed_options()
        before_options = json.loads(self.state.read_text())
        result = self.run_phase('68-migration.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        options = json.loads(self.state.read_text())
        self.assertTrue(all(value is None for value in options.values()))
        alters = [c['args'][0] for c in self.calls() if c['command'] == 'retention_sql'
                  and c['args'][0].startswith('ALTER TABLE')]
        self.assertEqual(len(alters), 2)
        for table in targets:
            self.assertTrue(any(table in sql and '= NULL' in sql for sql in alters))
            self.assertIn(f'`{table}` SET OPTIONS (partition_expiration_days = NULL)', result.stdout)
        path = self.work / 'deployments' / self.env['PROJECT'] / 'migration-sha256-current.json'
        record = json.loads(path.read_text())
        self.assertEqual({r['table'] for r in record['original_inventory']}, set(targets))
        # The exact metadata snapshots drive the same expiration query in
        # both cases; no test branch skips expiration after clearing.
        import duckdb
        conn = duckdb.connect()
        conn.execute('CREATE TABLE chunk (click_day DATE, volume INTEGER)')
        conn.execute('CREATE TABLE options (expiration_days DOUBLE)')
        def reland_and_expire(days):
            conn.execute('DELETE FROM chunk')
            conn.execute('DELETE FROM options')
            conn.execute('INSERT INTO options VALUES (?)', [days])
            conn.execute("INSERT INTO chunk VALUES (DATE '2026-06-01', 7)")
            conn.execute("""DELETE FROM chunk USING options
                WHERE expiration_days IS NOT NULL
                  AND click_day < DATE '2026-09-18' - expiration_days * INTERVAL 1 DAY""")
            return conn.execute('SELECT COALESCE(SUM(volume), 0) FROM chunk').fetchone()[0]
        self.assertEqual(reland_and_expire(before_options[targets[0]]), 0)
        self.assertEqual(reland_and_expire(options[targets[0]]), 7)
        conn.close()
        self.log.write_text('')
        result = self.run_phase('68-migration.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertFalse(any(c['command'] == 'retention_sql' and c['args'][0].startswith('ALTER TABLE')
                             for c in self.calls()))
        self.assertNotIn('SET OPTIONS (partition_expiration_days = NULL)', result.stdout)

    def test_phase89_refuses_missing_record_mismatch_lease_execution_and_protected(self):
        result = self.run_phase('89-retention.sh')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('88', result.stdout)
        self.rehearsal()
        for fault, values, needle in [('', {'PMAX_RETENTION_CONFIRMED': '90'}, 'PMAX_RETENTION_CONFIRMED'),
                                      ('lease', {}, 'lease'), ('execution', {}, 'execution')]:
            with self.subTest(fault=fault, values=values):
                self.log.write_text('')
                result = self.run_phase('89-retention.sh', U11_FAULT=fault, **values)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('phase 89 requires PMAX_RETENTION_CONFIRMED=91' if needle == 'PMAX_RETENTION_CONFIRMED' else needle, result.stdout)
                self.assertFalse(any(c['command'] == 'retention_sql' for c in self.calls()))
        self.seed_options(protected=True)
        result = self.run_phase('89-retention.sh')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('phase 89 retention preview or never-expire guard failed', result.stdout)
        self.assertFalse(any(c['command'] == 'retention_sql' and c['args'][0].startswith('ALTER TABLE')
                             for c in self.calls()))

    def test_phase89_retry_appends_without_replacing_original_inventory_and_rollback(self):
        targets = self.seed_options()
        rehearsal = self.rehearsal()
        result = self.run_phase('89-retention.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        path = self.work / 'deployments' / self.env['PROJECT'] / 'retention-sha256-current.json'
        first = json.loads(path.read_text())
        self.assertEqual([r['table'] for r in first['original_inventory']], targets)
        self.assertEqual(first['confirmed_value'], '91')
        self.assertEqual(first['phase_88_record'], str(rehearsal))
        from datetime import datetime, timedelta
        for row in first['original_inventory']:
            self.assertEqual(row['original_option'], 30)
            self.assertEqual(row['after_option'], 91)
            self.assertEqual(datetime.fromisoformat(row['rollback_deadline']) -
                             datetime.fromisoformat(row['alter_timestamp']), timedelta(hours=48))
        self.log.write_text('')
        result = self.run_phase('89-retention.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        second = json.loads(path.read_text())
        self.assertEqual(second['original_inventory'], first['original_inventory'])
        self.assertEqual(len(second['attempts']), 2)
        self.assertEqual(second['attempts'][-1]['tables_altered'], [])
        self.assertFalse(any(c['command'] == 'retention_sql' and c['args'][0].startswith('ALTER TABLE')
                             for c in self.calls()))
        result = subprocess.run([str(self.bin / 'uv'), 'run', 'pmax-pack', 'retention', '--rollback', str(path)],
                                env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count('ALTER TABLE'), 2)
        for table in targets:
            self.assertIn(f'`{table}` SET OPTIONS (partition_expiration_days = NULL)', result.stdout)

    def test_phase89_incremental_verifies_empty_inventory_on_first_deploy(self):
        self.set_storage('incremental')
        self.seed_options(differing=False)
        self.rehearsal(anchor_rehearsal_applicable=False,
                       anchor_rehearsal_status='not applicable: no previous image',
                       anchor_scripts_passed=None, cohort_counting_restored=None)
        result = self.run_phase('89-retention.sh', UPGRADE='0')
        self.assertEqual(result.returncode, 0, result.stdout)
        path = self.work / 'deployments' / self.env['PROJECT'] / 'retention-sha256-current.json'
        self.assertEqual(json.loads(path.read_text())['original_inventory'], [])
        self.assertFalse(any(c['command'] == 'retention_sql' and c['args'][0].startswith('ALTER TABLE')
                             for c in self.calls()))


    def test_phase68_plan_prints_idle_and_incremental_leaves_without_mutation(self):
        self.set_storage('incremental')
        result = self.run_phase('68-migration.sh', PLAN='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        text = result.stdout.replace('\\', '')
        for leaf in ('gcloud scheduler jobs describe pmax-pack-daily',
                     'gcloud storage objects describe gs://test-report-bucket/lease.json',
                     'gcloud storage cat gs://test-report-bucket/lease.json',
                     'gcloud run jobs executions list --job=pmax-pack-daily',
                     '--format=json', '--format=value(state)', '--location=europe-west1',
                     '--region=europe-west1', 'pmax-pack retention', '--confirmed never',
                     '--record', '--digest', '--phase-88-record'):
            self.assertIn(leaf, text)
        self.assertFalse(any(c['command'] in ('gcloud', 'bq', 'retention_sql') or
                             (c['command'] == 'uv' and 'pmax-pack' in c['args']) for c in self.calls()))
        self.assertFalse((self.work / 'deployments').exists())

    def test_phase89_plan_and_invalid_rehearsal_records_never_apply(self):
        result = self.run_phase('89-retention.sh', PLAN='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        for flag in ('--apply', '--confirmed 91', '--record', '--digest', '--phase-88-record', '--rollback'):
            self.assertIn(flag, result.stdout)
        self.assertFalse(any(c['command'] in ('gcloud', 'bq', 'retention_sql') for c in self.calls()))
        for field, value in [('image_digest', 'test-image@sha256:other'), ('target_dataset', 'pmax_marts'),
                             ('confirmed_value', '90'), ('rehearsal_passed', False),
                             ('option_map_asserted', False), ('never_expire_asserted', False),
                             ('cohort_counting_restored', False), ('anchor_scripts_passed', False),
                             ('looker_probes_passed', False)]:
            with self.subTest(field=field):
                self.rehearsal(**{field: value})
                self.log.write_text('')
                result = self.run_phase('89-retention.sh')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('phase-88', result.stdout)
                self.assertFalse(any(c['command'] in ('gcloud', 'bq', 'retention_sql') for c in self.calls()))
        self.rehearsal()
        self.set_storage('incremental')
        self.rehearsal()
        self.seed_options()
        result = self.run_phase('89-retention.sh')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('all NULL', result.stdout)
        self.assertFalse(any(c['command'] == 'retention_sql' and c['args'][0].startswith('ALTER TABLE')
                             for c in self.calls()))

    def test_phase25_upgrade_defers_dry_run_to_migration(self):
        result = self.run_phase('25-dry-run.sh', PLAN='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertNotIn('--dry-run', result.stdout)
        self.assertIn('phase 68', result.stdout)


    def test_config_generation_missing_or_invalid_refuses_record(self):
        for response in ('{}', 'not-json', '{"generation":0}', '{"generation":-1}', '{"generation":"bad"}'):
            with self.subTest(response=response):
                result = self.run_phase('65-config.sh', U11_GENERATION_JSON=response)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('generation', result.stdout)
                self.assertFalse((self.work / 'deployments').exists())

    def test_idle_gate_fails_closed_on_malformed_metadata(self):
        for fault in ('lease_malformed', 'execution_malformed', 'execution_null'):
            with self.subTest(fault=fault):
                result = subprocess.run(['bash', '-c',
                    'set -euo pipefail; source "$1"; assert_ladder_idle 68-migration 1',
                    'bash', str(self.functions)], env={**self.env, 'U11_FAULT': fault},
                    stdin=subprocess.DEVNULL, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('lease' if fault.startswith('lease') else 'execution', result.stdout + result.stderr)
        # A terminal failed execution is complete and may not block migration.
        result = subprocess.run(['bash', '-c',
            'set -euo pipefail; source "$1"; assert_ladder_idle 68-migration 1',
            'bash', str(self.functions)], env={**self.env, 'U11_FAULT': 'execution_failed'},
            stdin=subprocess.DEVNULL, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


    def test_pass2_preflight_validates_anchor_before_credential_probe(self):
        self.alert_proof()
        self.anchor_record()
        checkout = self.work / 'anchor'
        source = checkout / 'src' / 'pmax_pack'
        source.mkdir(parents=True)
        (source / 'manifest.yaml').write_text('steps: []\n')
        saved = self.work / 'anchor.yaml'
        saved.write_text(self.config.read_text())
        valid = {'CONFIG_FILE': '', 'CONFIG_LOCAL': str(self.work / 'validated.yaml'),
                 'PMAX_SIGNED_REVIEW': str(self.signed), 'PMAX_ALERT_CONFIRMED': '1',
                 'PMAX_SKIPPED_ALERT_SILENT': '1', 'PMAX_NOTIFICATION_CHANNEL': 'test-channel',
                 'PMAX_ANCHOR_CHECKOUT': str(checkout), 'PMAX_ANCHOR_CONFIG': str(saved)}
        for missing in (source / 'cli.py', checkout / 'pyproject.toml'):
            with self.subTest(missing=missing.name):
                (source / 'cli.py').write_text('# anchor fixture\n')
                (checkout / 'pyproject.toml').write_text('[project]\nname="anchor"\n')
                missing.unlink()
                self.log.write_text('')
                result = self.run_phase('00-preflight.sh', **valid)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('PMAX_ANCHOR_CHECKOUT', result.stdout)
                self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))
        (checkout / 'pyproject.toml').write_text('[project]\nname="anchor"\n')
        data = yaml.safe_load(saved.read_text())
        data['accounts'] = ['2345678901']
        saved.write_text(yaml.safe_dump(data))
        self.log.write_text('')
        result = self.run_phase('00-preflight.sh', **valid)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('accounts', result.stdout)
        self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))


    def test_config_content_drift_since_preflight_refuses_both_deploy_modes(self):
        for upgrade in ('0', '1'):
            with self.subTest(upgrade=upgrade):
                self.log.write_text('')
                result = self.run_phase('65-config.sh', UPGRADE=upgrade, U11_FAULT='config_drift')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('config', result.stdout.lower())
                self.assertFalse((self.work / 'deployments').exists())

    def test_config_generation_changes_during_verification_refuse_both_deploy_modes(self):
        for upgrade in ('0', '1'):
            with self.subTest(upgrade=upgrade):
                self.log.write_text('')
                result = self.run_phase('65-config.sh', UPGRADE=upgrade, U11_FAULT='config_generation_drift')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('generation', result.stdout.lower())
                self.assertFalse((self.work / 'deployments').exists())


    def anchor_record(self):
        path = self.work / 'deployments' / self.env['PROJECT'] / 'rollback-anchor.txt'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('anchor_source_commit=' + 'a' * 40 + '\nanchor_public_commit=' + 'b' * 40 +
                        '\nanchor_digest=test-image@sha256:anchor\n')
        return path

    def signed_inputs(self):
        self.anchor_record()
        self.alert_proof()
        marker = self.work/'deployments'/self.env['PROJECT']/'resume-evidence-sha256-current.json'
        completed = False
        if marker.exists():
            try:
                value = json.loads(marker.read_text())
                completed = isinstance(value, dict) and value.get('resumed') is True and value.get('generation') == 'f'*32
            except ValueError: pass
        return {'CONFIG_FILE': '', 'CONFIG_LOCAL': str(self.work / 'validated.yaml'),
                'PMAX_SIGNED_REVIEW': str(self.signed), 'PMAX_ALERT_CONFIRMED': '' if completed else '1',
                'PMAX_SKIPPED_ALERT_SILENT': '' if completed else '1', 'PMAX_NOTIFICATION_CHANNEL': 'test-channel'}

    def run_idle(self, **values):
        return subprocess.run(['bash', '-c',
            'set -euo pipefail; source "$1"; assert_ladder_idle 68-migration 1',
            'bash', str(self.functions)], env={**self.env, **values},
            stdin=subprocess.DEVNULL, capture_output=True, text=True)

    def test_assume_yes_never_authorizes_migration_or_retention(self):
        for phase in ('68-migration', '89-retention'):
            with self.subTest(phase=phase):
                result = subprocess.run(['bash', '-c', 'source "$1"; confirm_human_phase "$2"',
                    'bash', str(self.functions), phase], env={**self.env, 'ASSUME_YES': '1',
                    'PMAX_CONFIRMED_PHASES': ''}, stdin=subprocess.DEVNULL, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f'human-owned phase {phase} needs the operator', result.stderr)

    def test_signed_pass_requires_89_before_consuming_signature(self):
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                                PMAX_CONFIRMED_PHASES='68-migration,95-resume')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('signed pass requires 89-retention', result.stdout)
        self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))

    def test_signed_pass_additional_inputs_have_specific_refusals(self):
        valid = self.signed_inputs()
        for key, value, needle in [
            ('PMAX_NOTIFICATION_CHANNEL', '', 'signed pass requires PMAX_NOTIFICATION_CHANNEL'),
            ('PMAX_SIGNED_REVIEW', str(self.work / 'missing.yaml'), 'PMAX_SIGNED_REVIEW must name'),
            ('PMAX_RETENTION_CONFIRMED', '90', 'signed pass requires PMAX_RETENTION_CONFIRMED=91'),
        ]:
            with self.subTest(key=key):
                result = self.run_phase('00-preflight.sh', **{**valid, key: value})
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(needle, result.stdout)
        self.set_storage('incremental')
        result = self.run_phase('00-preflight.sh', **valid, PMAX_RETENTION_CONFIRMED='91')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('signed pass requires PMAX_RETENTION_CONFIRMED=never', result.stdout)

    def test_first_deploy_signed_pass_does_not_require_anchor(self):
        valid = self.signed_inputs()
        result = self.run_phase('00-preflight.sh', **{**valid, 'UPGRADE': '0',
            'CONFIG_FILE': str(self.config), 'PMAX_ANCHOR_CHECKOUT': '', 'PMAX_ANCHOR_CONFIG': ''})
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertFalse(any(c['command'] == 'git' for c in self.calls()))

    def test_signed_upgrade_requires_verified_anchor_commit(self):
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(), U11_FAULT='wrong_anchor')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('anchor source commit', result.stdout)
        self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))

    def test_idle_gate_accepts_large_complete_execution_history(self):
        result = self.run_idle(U11_FAULT='large_execution_list')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_idle_gate_rejects_empty_or_failed_classifier(self):
        directory = self.work / 'classifier'
        directory.mkdir()
        for body in ('return 0', 'return 1'):
            with self.subTest(body=body):
                (directory / 'execution-poll.sh').write_text('classify_execution_state() { ' + body + '; }\n')
                result = self.run_idle(PHASE_ROOT=str(directory))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('execution classifier', result.stdout + result.stderr)

    def test_migration_review_refusal_names_only_phase68(self):
        result = self.run_idle(PMAX_MIGRATION_REVIEWED='0')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('68-migration requires PMAX_MIGRATION_REVIEWED=1', result.stderr)
        self.assertNotIn('phases 68 and 89', result.stderr)

    def test_omitted_storage_first_deploy_survives_second_invocation(self):
        data = yaml.safe_load(self.config.read_text())
        data.pop('storage')
        self.config.write_text(yaml.safe_dump(data))
        first_local = self.work / 'first-validated.yaml'
        result = self.run_phase('00-preflight.sh', UPGRADE='0', CONFIG_LOCAL=str(first_local))
        self.assertEqual(result.returncode, 0, result.stdout)
        result = self.run_phase('65-config.sh', UPGRADE='0', CONFIG_LOCAL=str(first_local))
        self.assertEqual(result.returncode, 0, result.stdout)
        record_dir = self.work / 'deployments' / self.env['PROJECT']
        record_dir.mkdir(parents=True, exist_ok=True)
        (record_dir / 'first-run-evidence-sha256-current.json').write_text(json.dumps({
            'mode': 'run', 'status': 'SUCCESS', 'image_digest': self.env['IMAGE_REF'], 'run_id': 'first-run-fixture'}))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                                PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(yaml.safe_load(Path(self.env['U11_REMOTE_CONFIG']).read_text()).get('storage'), 'window')
        (record_dir / 'resume-evidence-sha256-current.json').write_text(json.dumps({
            'image_digest': self.env['IMAGE_REF'], 'generation': 'f'*32, 'resumed': True}))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                                PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('PMAX_ANCHOR_CHECKOUT', result.stdout)
        remote_config = Path(self.env['U11_REMOTE_CONFIG'])
        data = yaml.safe_load(remote_config.read_text())
        data.pop('storage')
        remote_config.write_text(yaml.safe_dump(data))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs())
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('upgrade requires an explicit storage key', result.stdout)


    def test_phase89_signed_review_refuses_before_retention_calls(self):
        self.rehearsal()
        result = self.run_phase('89-retention.sh', REVIEW_RECORDED='0')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('phase 89 requires the signed review from phase 85', result.stdout)
        self.assertFalse(any(c['command'] in ('bq', 'retention_sql') or
                             (c['command'] == 'uv' and 'retention' in c['args']) for c in self.calls()))

    def test_phase68_incremental_numeric_confirmation_refuses_before_mutation(self):
        self.set_storage('incremental')
        result = self.run_phase('68-migration.sh', PMAX_RETENTION_CONFIRMED='91')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('phase 68 incremental clearing requires PMAX_RETENTION_CONFIRMED=never', result.stdout)
        self.assertFalse(any(c['command'] in ('bq', 'retention_sql') or
                             (c['command'] == 'uv' and 'pmax-pack' in c['args']) for c in self.calls()))

    def test_phase68_fixed_drop_names_are_literal_and_not_folder_derived(self):
        source = (root / 'deploy/phases/68-migration.sh').read_text()
        match = re.search(r'for view in (.*?);\s*do', source, re.S)
        self.assertIsNotNone(match, 'fixed literal DROP loop is missing')
        words = shlex.split(match.group(1).replace('\\\n', ' '))
        self.assertEqual(words, ['v_asset_performance', 'v_campaign_truth', 'v_cohort_asset',
            'v_cohort_asset_group', 'v_cohort_campaign', 'v_performance_asset',
            'v_performance_asset_group', 'v_performance_campaign'])
        self.assertNotRegex(match.group(1), r'\$|\*|\?|\bls\b|\bfind\b')

    def test_phase68_combined_faults_pin_refusal_order(self):
        for fault, reviewed, expected, forbidden in [
            ('scheduler', '0', 'requires a PAUSED Scheduler', 'PMAX_MIGRATION_REVIEWED'),
            ('lease_execution', '0', 'requires PMAX_MIGRATION_REVIEWED=1', 'live lease:'),
            ('lease_execution', '1', 'live lease:', 'running execution'),
        ]:
            with self.subTest(fault=fault, reviewed=reviewed):
                self.log.write_text('')
                result = self.run_phase('68-migration.sh', U11_FAULT=fault, PMAX_MIGRATION_REVIEWED=reviewed)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(expected, result.stdout)
                self.assertNotIn(forbidden, result.stdout)
                self.assertFalse(any(c['args'][:4] == ['run', 'jobs', 'executions', 'list'] for c in self.calls()))
                self.assertFalse(any(c['command'] in ('bq', 'retention_sql') for c in self.calls()))

    def test_retention_operator_identity_is_captured_and_impersonation_cleared(self):
        self.set_storage('incremental')
        self.seed_options()
        result = self.run_phase('68-migration.sh', CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=self.env['RUNTIME_SA'])
        self.assertEqual(result.returncode, 0, result.stdout)
        migration = self.work / 'deployments' / self.env['PROJECT'] / 'migration-sha256-current.json'
        record = json.loads(migration.read_text())
        self.assertEqual(record.get('retention_operator_account'), 'operator@example.test')
        self.assertEqual(record.get('retention_operator_impersonation'), '')
        self.assertIs(record.get('retention_operator_adc_verified'), True)
        self.assertEqual(record.get('phase_88_record'), str(migration) + ' (pre-88 clearing)')
        self.rehearsal()
        result = self.run_phase('89-retention.sh', CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=self.env['RUNTIME_SA'])
        self.assertEqual(result.returncode, 0, result.stdout)
        record = json.loads((migration.parent / 'retention-sha256-current.json').read_text())
        self.assertEqual(record.get('retention_operator_account'), 'operator@example.test')
        self.assertEqual(record.get('retention_operator_impersonation'), '')
        self.assertIs(record.get('retention_operator_adc_verified'), True)
        self.assertIs(record['attempts'][-1].get('retention_operator_adc_verified'), True)
        retention_calls = [c for c in self.calls() if c['command'] == 'uv' and 'retention' in c['args']]
        self.assertTrue(retention_calls)
        self.assertTrue(all(c['impersonation'] == '' for c in retention_calls), retention_calls)

    def test_runtime_or_empty_account_cannot_apply_retention(self):
        self.rehearsal()
        for phase in ('68-migration.sh', '89-retention.sh'):
            for fault in ('runtime_operator', 'empty_operator'):
                with self.subTest(phase=phase, fault=fault):
                    self.log.write_text('')
                    result = self.run_phase(phase, U11_FAULT=fault)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn('retention operator account must be set and differ from the runtime service account', result.stdout)
                    self.assertFalse(any(c['command'] in ('bq', 'retention_sql') for c in self.calls()))

    def test_phase68_live_ddl_labels_and_execution_heading(self):
        result = self.run_phase('68-migration.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        queries = [c['args'] for c in self.calls() if c['command'] == 'bq']
        for args in queries:
            for label in ('--label=app:pmax', '--label=env:prod', '--label=stage:ladder-68'):
                self.assertIn(label, args)
            self.assertTrue(any(re.fullmatch(r'--label=run_id:[a-z0-9_-]+', arg) for arg in args))
            self.assertTrue(any(arg.startswith('--maximum_bytes_billed=') for arg in args))
        self.assertEqual(sum(line.startswith('EXECUTE  ') for line in result.stdout.splitlines()), 8)

    def test_anchor_cohort_preflight_requires_matching_ops_dataset(self):
        saved = self.work / 'anchor-other-ops.yaml'
        data = yaml.safe_load(self.config.read_text())
        data['datasets']['ops'] = 'pmax_rehearsal_ops'
        saved.write_text(yaml.safe_dump(data))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(), PMAX_ANCHOR_CONFIG=str(saved))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('anchor config datasets.ops mismatch', result.stdout)
        self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))


    def test_signed_upgrade_accepts_recorded_public_anchor_commit(self):
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(), U11_FAULT='public_anchor',
                                extra='printf "ANCHOR_KIND=%s\\n" "$ANCHOR_SOURCE_KIND"')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('ANCHOR_KIND=public', result.stdout)
        self.assertTrue(any(c['command'] == 'git' for c in self.calls()))



    def first_deploy_progress(self, stage, image=None):
        image = image or self.env['IMAGE_REF']
        key = image.rsplit('@', 1)[-1].replace(':', '-')
        directory = self.work / 'deployments' / self.env['PROJECT']
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f'first-run-evidence-{key}.json').write_text(json.dumps({
            'image_digest': image, 'status': 'SUCCESS', 'run_id': 'first-run-fixture', 'mode': 'run'}))
        if stage >= 85:
            (directory / f'signed-review-validation-{key}.json').write_text(json.dumps({
                'validated': True, 'image_digest': image, 'run_id': 'first-run-fixture'}))
        if stage >= 89:
            (directory / f'rehearsal-evidence-{key}.json').write_text(json.dumps({
                'rehearsal_passed': True, 'image_digest': image, 'anchor_rehearsal_applicable': False}))
            (directory / f'retention-{key}.json').write_text(json.dumps({'digest': image, 'attempts': []}))
        return directory

    def test_first_deploy_retry_after_88_and_90_keeps_continuation(self):
        for failed_phase in (88, 90):
            with self.subTest(failed_phase=failed_phase):
                self.first_deploy_progress(failed_phase - 1)
                result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                    PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='',
                    extra='printf "CONTINUATION=%s ANCHOR_REQUIRED=%s\\n" "$FIRST_DEPLOY_CONTINUATION" "$ANCHOR_REHEARSAL_REQUIRED"')
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertIn('CONTINUATION=1 ANCHOR_REQUIRED=0', result.stdout)

    def test_continuation_missing_remote_storage_uses_window_default(self):
        self.first_deploy_progress(85)
        data = yaml.safe_load(self.config.read_text())
        data.pop('storage')
        Path(self.env['U11_REMOTE_CONFIG']).write_text(yaml.safe_dump(data))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
            PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='',
            extra='printf "CONTINUATION=%s STORAGE=%s\\n" "$FIRST_DEPLOY_CONTINUATION" "$STORAGE"')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('CONTINUATION=1 STORAGE=window', result.stdout)
        self.assertNotIn('storage', yaml.safe_load(Path(self.env['U11_REMOTE_CONFIG']).read_text()))

    def test_resume_completion_is_digest_bound_and_new_digest_needs_anchor(self):
        directory = self.first_deploy_progress(90)
        unrelated = directory / 'resume-evidence-sha256-older.json'
        unrelated.write_text(json.dumps({'image_digest': 'test-image@sha256:older', 'resumed': True}))
        result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                                PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='')
        self.assertEqual(result.returncode, 0, result.stdout)
        (directory / 'resume-evidence-sha256-current.json').write_text(json.dumps({
            'image_digest': self.env['IMAGE_REF'], 'generation': 'f'*32, 'resumed': True}))
        for image in (self.env['IMAGE_REF'], self.env['IMAGE_REF'].replace(':current', ':new')):
            with self.subTest(image=image):
                inputs = self.signed_inputs()
                if image != self.env['IMAGE_REF']:
                    old = self.env['IMAGE_REF']
                    self.env['IMAGE_REF'] = image
                    proof = self.alert_proof()
                    proof.rename(proof.with_name('alert-proof-sha256-new.json'))
                    self.env['IMAGE_REF'] = old
                result = self.run_phase('00-preflight.sh', **inputs,
                    IMAGE_REF=image, PMAX_IMAGE_REF=image, PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('signed pass requires PMAX_ANCHOR_CHECKOUT', result.stdout)

    def test_forged_or_malformed_resume_marker_refuses(self):
        directory = self.first_deploy_progress(90)
        marker = directory / 'resume-evidence-sha256-current.json'
        shapes = {
            'wrong_digest': json.dumps({'image_digest': 'test-image@sha256:older', 'resumed': True}),
            'resumed_false': json.dumps({'image_digest': self.env['IMAGE_REF'], 'resumed': False}),
            'resumed_string': json.dumps({'image_digest': self.env['IMAGE_REF'], 'resumed': 'true'}),
            'list': json.dumps([{'image_digest': self.env['IMAGE_REF'], 'resumed': True}]),
            'malformed': '{"image_digest": ',
        }
        for shape, text in shapes.items():
            with self.subTest(shape=shape):
                marker.write_text(text)
                result = self.run_phase('00-preflight.sh', **self.signed_inputs(),
                                        PMAX_ANCHOR_CHECKOUT='', PMAX_ANCHOR_CONFIG='')
                combined = result.stdout
                self.assertNotEqual(result.returncode, 0, combined)
                self.assertIn('ladder continuation state is malformed' if shape in ('list', 'malformed')
                              else 'invalid continuation completion evidence', combined)
                self.assertNotIn('Traceback', combined)
                self.assertNotIn('signed pass requires PMAX_ANCHOR_CHECKOUT', combined)

    def test_anchor_checkout_rejects_tracked_and_untracked_code(self):
        for fault in ('anchor_tracked', 'anchor_untracked_src', 'anchor_untracked_deploy', 'anchor_untracked_root'):
            with self.subTest(fault=fault):
                self.log.write_text('')
                result = self.run_phase('00-preflight.sh', **self.signed_inputs(), U11_FAULT=fault)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('anchor checkout must be clean', result.stdout)
                self.assertFalse(any(c['command'] == 'uv' and 'probe' in c['args'] for c in self.calls()))

    def test_anchor_checkout_status_errors_fail_closed(self):
        for fault in ('anchor_status_error',):
            with self.subTest(fault=fault):
                result = self.run_phase('00-preflight.sh', **self.signed_inputs(), U11_FAULT=fault)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('phase capture failed', result.stdout)

    def test_resume_marker_only_after_successful_live_phase95(self):
        ladder = self.work / 'ladder'
        phases = ladder / 'deploy' / 'phases'
        phases.mkdir(parents=True)
        source = (root / 'deploy/deploy.sh').read_text()
        driver = ladder / 'deploy' / 'deploy.sh'
        driver.write_text(source)
        specs = re.findall(r'^  "([0-9]+-[^|]+)\|[^\"]+"$', source, re.M)
        self.assertIn('95-resume', specs)
        for phase in specs:
            (phases / (phase + '.sh')).write_text(':\n')
        self.seed_continuation(ladder, upgrade=False)
        (phases / '25-dry-run.sh').write_text('LADDER_GENERATION='+('f'*32)+'; export LADDER_GENERATION\n')
        marker = ladder / 'deployments' / self.env['PROJECT'] / 'resume-evidence-sha256-current.json'
        command = ['bash', str(driver), '--project', self.env['PROJECT'], '--region', self.env['REGION'],
                   '--config-uri', self.env['CONFIG_URI'], '--credential-file', str(self.signed)]
        env = {**self.env, 'PMAX_CONFIRMED_PHASES': ','.join(p for p in specs if p != '85-review')}
        (phases / '75-lease-drill.sh').write_text('record_pass1_skipped_execution() { :; }\n')
        (phases / '95-resume.sh').write_text('return 1\n')
        result = subprocess.run(command, env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(marker.exists())
        (phases / '95-resume.sh').write_text(':\n')
        result = subprocess.run(command + ['--plan'], env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(marker.exists())
        result = subprocess.run(command, env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(marker.exists(), 'successful live phase95 must persist a completion marker')
        record = json.loads(marker.read_text())
        self.assertEqual(record['image_digest'], self.env['IMAGE_REF'])
        self.assertIs(record['resumed'], True)



    def test_idle_gate_accepts_native_and_string_count_cells(self):
        for succeeded, failed in ((1, 0), ('1', '0'), (None, '1')):
            with self.subTest(succeeded=succeeded, failed=failed):
                rows = [{'status': {'completionTime': '2026-09-18T00:00:00Z',
                                   'succeededCount': succeeded, 'failedCount': failed}}]
                result = self.run_idle(U11_EXECUTIONS_JSON=json.dumps(rows))
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_idle_gate_rejects_malformed_status_cells(self):
        for field, value in [('completionTime', True), ('completionTime', 1),
                             ('completionTime', {}), ('succeededCount', True),
                             ('succeededCount', 1.5), ('succeededCount', -1),
                             ('succeededCount', 'invalid'), ('succeededCount', {}),
                             ('failedCount', False), ('failedCount', ''), ('failedCount', '-1')]:
            with self.subTest(field=field, value=value):
                status = {'completionTime': '2026-09-18T00:00:00Z', 'succeededCount': 1, 'failedCount': 0}
                status[field] = value
                result = self.run_idle(U11_EXECUTIONS_JSON=json.dumps([{'status': status}]))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('invalid execution list', result.stdout + result.stderr)



    def alert_proof(self, **changes):
        key=self.env['IMAGE_REF'].rsplit('@',1)[-1].replace(':','-')
        path = self.work / 'deployments' / self.env['PROJECT'] / f'alert-proof-{key}.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        state_path = path.parent/'ladder-continuation.json'
        if not state_path.exists():
            # Preflight validates the active generation before accepting flags.
            first_deploy = (path.parent/'first-run-evidence-sha256-current.json').exists()
            self.seed_continuation(upgrade=not first_deploy)
            state = json.loads(state_path.read_text()); state['baselines'] = {}
            state_path.write_text(json.dumps(state))
        path.write_text(json.dumps({'image_digest': self.env['IMAGE_REF'],
            'generation': 'f'*32,
            'failed_execution_id': 'failed-probe-execution',
            'submitted_at': '2026-09-18T01:00:00+00:00',
            'recorded_at': '2026-09-18T01:01:00+00:00',
            'skipped_execution_id': 'skipped-pass1-execution',
            'skipped_observed_at': '2026-09-18T00:00:00+00:00',
            'skipped_run_id': 'ldc-fixture', **changes}))
        (path.parent / f'alert-submission-{key}.json').write_text(json.dumps({
            'image_digest': self.env['IMAGE_REF'], 'generation': 'f'*32,
            'failed_execution_id': 'failed-probe-execution',
            'submitted_at': '2026-09-18T01:00:00+00:00'}))
        self.lease_proof()
        return path

    def lease_proof(self):
        key=self.env['IMAGE_REF'].rsplit('@',1)[-1].replace(':','-')
        path = self.work / 'deployments' / self.env['PROJECT'] / f'lease-drill-evidence-{key}-pass1.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'image_digest': self.env['IMAGE_REF'],
            'generation': 'f'*32,
            'skipped_execution_id': 'skipped-pass1-execution', 'skipped_run_id': 'ldc-fixture',
            'skipped_observed_at': '2026-09-18T00:00:00+00:00'}))
        return path

    def test_followup_signed_pass_without_flags_reaches_alert_and_stops_at_resume(self):
        inputs = {**self.signed_inputs(), 'PLAN': '0', 'PMAX_ALERT_CONFIRMED': '',
                  'PMAX_SKIPPED_ALERT_SILENT': '', 'PMAX_CONFIRMED_PHASES': '68-migration,89-retention'}
        proof=self.alert_proof()
        proof.unlink()
        proof.with_name('alert-submission-sha256-current.json').unlink()
        result = self.run_phase('00-preflight.sh', **inputs)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.lease_proof()
        result = self.run_phase('90-alert.sh', PMAX_NOTIFICATION_CHANNEL='test-channel',
                                PMAX_ALERT_CONFIRMED='', PMAX_SKIPPED_ALERT_SILENT='')
        self.assertEqual(result.returncode, 0, result.stdout)
        proof = json.loads((self.work / 'deployments' / self.env['PROJECT'] / 'alert-proof-sha256-current.json').read_text())
        self.assertEqual(proof['failed_execution_id'], 'failed-probe-execution')
        self.assertEqual(sum(c['args'][:3] == ['run','jobs','execute'] for c in self.calls()), 1)
        self.assertEqual(proof['skipped_execution_id'], 'skipped-pass1-execution')
        result = self.run_phase('95-resume.sh', UPGRADE='0', ALERT_PROVEN='1')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('95-resume requires PMAX_ALERT_CONFIRMED=1 after alert-proof', result.stdout)
        self.assertFalse(any(c['args'][:3] == ['scheduler', 'jobs', 'resume'] for c in self.calls()))

    def test_followup_flags_without_prior_alert_proof_refuse(self):
        inputs = self.signed_inputs()
        self.alert_proof().unlink()
        result = self.run_phase('00-preflight.sh', **inputs)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('alert confirmation flags require an existing alert-proof', result.stdout)
        result = self.run_phase('95-resume.sh', UPGRADE='0', ALERT_PROVEN='1',
                                PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('alert-proof', result.stdout)
        self.assertFalse(any(c['args'][:3] == ['scheduler', 'jobs', 'resume'] for c in self.calls()))

    def test_followup_existing_proof_and_later_flags_resume_without_new_drill(self):
        proof = self.alert_proof()
        before = proof.read_bytes()
        result = self.run_phase('90-alert.sh',
            extra='source "$PHASE_ROOT/95-resume.sh"', UPGRADE='0',
            PMAX_NOTIFICATION_CHANNEL='test-channel', PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(proof.read_bytes(), before)
        self.assertFalse(any(c['args'][:3] == ['run', 'jobs', 'execute'] for c in self.calls()))
        self.assertTrue(any(c['args'][:3] == ['scheduler', 'jobs', 'resume'] for c in self.calls()))

    def test_followup_alert_proof_refuses_wrong_digest_or_bad_time(self):
        for changes in ({'image_digest':'test-image@sha256:other'}, {'submitted_at':'invalid'},
                        {'recorded_at':'2099-01-01T00:00:00+00:00'}, {'skipped_execution_id':''}):
            with self.subTest(changes=changes):
                self.alert_proof(**changes)
                result = self.run_phase('95-resume.sh', UPGRADE='0', ALERT_PROVEN='1',
                    PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('alert-proof', result.stdout)

    def test_followup_alert_submission_error_or_success_cannot_create_proof(self):
        self.lease_proof()
        for fault in ('alert_submit_error', 'alert_describe_error', 'alert_succeeded', 'alert_running'):
            with self.subTest(fault=fault):
                result = self.run_phase('90-alert.sh', PMAX_NOTIFICATION_CHANNEL='test-channel', U11_FAULT=fault)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertFalse((self.work / 'deployments' / self.env['PROJECT'] / 'alert-proof-sha256-current.json').exists())

    def test_followup_adc_mismatch_or_unreadable_identity_refuses_before_mutation(self):
        for fault in ('adc_mismatch', 'adc_network', 'adc_missing_email', 'adc_unverified_email', 'adc_service_email'):
            with self.subTest(fault=fault):
                self.log.write_text('')
                result = self.run_phase('68-migration.sh', U11_FAULT=fault)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('ADC', result.stdout)
                self.assertNotIn('operator@example.test', result.stdout)
                self.assertNotIn('TEST_ADC_TOKEN_NEVER_PRINT', result.stdout)
                self.assertFalse(any(c['command'] in ('bq','retention_sql') for c in self.calls()))

    def test_followup_non_user_adc_refuses_before_token_or_mutation(self):
        for credential_type in ('impersonated_service_account', 'service_account', 'external_account'):
            with self.subTest(credential_type=credential_type):
                self.log.write_text('')
                self.adc.write_text(json.dumps({'type': credential_type}))
                result = self.run_phase('68-migration.sh')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('ADC must be a user credential', result.stdout)
                self.assertFalse(any(c['command'] in ('bq','retention_sql','adc_tokeninfo') for c in self.calls()))

    def test_followup_matching_adc_is_recorded_on_migration(self):
        result = self.run_phase('68-migration.sh')
        self.assertEqual(result.returncode, 0, result.stdout)
        record = json.loads((self.work / 'deployments' / self.env['PROJECT'] / 'migration-sha256-current.json').read_text())
        self.assertIs(record.get('retention_operator_adc_verified'), True)
        self.assertTrue(any(c['command'] == 'adc_tokeninfo' for c in self.calls()))
        self.assertNotIn('operator@example.test', result.stdout)
        self.assertNotIn('TEST_ADC_TOKEN_NEVER_PRINT', result.stdout)



    def test_followup_resume_uses_pass1_even_if_active_pointer_was_replaced(self):
        self.alert_proof()
        directory = self.work / 'deployments' / self.env['PROJECT']
        before = [{'row_count':'20', 'observed_days':'3', 'latest_observed_day':'2026-09-18'}]
        (directory / f"observation-before-sha256-current-{'f' * 32}-pass1.json").write_text(json.dumps(before))
        (directory / 'observation-before.json').write_text(json.dumps([
            {'row_count':'1', 'observed_days':'1', 'latest_observed_day':'2026-09-01'}]))
        result = self.run_phase('95-resume.sh', ALERT_PROVEN='1', PMAX_ALERT_CONFIRMED='1',
            PMAX_SKIPPED_ALERT_SILENT='1', OBSERVATION_SQL='SELECT row_count FROM raw_observations',
            OBSERVATION_PASS1_RECORD=str(directory / f"observation-before-sha256-current-{'f' * 32}-pass1.json"))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('observation log regressed: row_count', result.stdout)
        self.assertFalse(any(c['args'][:3] == ['scheduler','jobs','resume'] for c in self.calls()))

    def test_followup_resume_records_both_observation_triples(self):
        self.alert_proof()
        directory = self.work / 'deployments' / self.env['PROJECT']
        before = [{'row_count':'10', 'observed_days':'2', 'latest_observed_day':'2026-09-17'}]
        (directory / f"observation-before-sha256-current-{'f' * 32}-pass1.json").write_text(json.dumps(before))
        active = [{'row_count':'11', 'observed_days':'3', 'latest_observed_day':'2026-09-18'}]
        (directory / 'observation-before.json').write_text(json.dumps(active))
        (directory / f"observation-before-sha256-current-{'f' * 32}-pass2.json").write_text(json.dumps(active))
        self.seed_continuation()
        result = self.run_phase('95-resume.sh', extra='record_resume_completion',
            ALERT_PROVEN='1', PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1',
            OBSERVATION_SQL='SELECT row_count FROM raw_observations',
            OBSERVATION_PASS1_RECORD=str(directory / f"observation-before-sha256-current-{'f' * 32}-pass1.json"))
        self.assertEqual(result.returncode, 0, result.stdout)
        record = json.loads((directory / 'resume-evidence-sha256-current.json').read_text())
        self.assertEqual(record['observation_gate']['before'], before[0])
        self.assertEqual(record['observation_gate']['active'], active[0])
        self.assertEqual(record['observation_gate']['after'],
            {'row_count':'12', 'observed_days':'3', 'latest_observed_day':'2026-09-18'})
        self.assertTrue(record['alert_confirmed_at'])

    def test_followup_pass1_skip_record_binds_execution_and_is_preserved(self):
        command = ['bash', '-c', 'set -euo pipefail; source "$1"; record_pass1_skipped_execution',
                   'bash', str(self.functions)]
        env = {**self.env, 'LADDER_GENERATION':'f'*32, 'PMAX_ENV':'prod', 'LEASE_PHASE_STARTED_AT':'2026-09-18T00:00:00Z',
               'OWNER_RUN_ID':'ldo-fixture', 'OWNER_EXECUTION':'owner-fixture',
               'CONTENDER_RUN_ID':'ldc-fixture', 'CONTENDER_EXECUTION':'contender-fixture'}
        result = subprocess.run(command, env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        path = self.work / 'deployments' / self.env['PROJECT'] / 'lease-drill-evidence-sha256-current-pass1.json'
        record = json.loads(path.read_text())
        self.assertEqual(record['skipped_execution_id'], 'contender-fixture')
        original = path.read_bytes()
        retry_env = {**env, 'LADDER_PASS_NUMBER': '2', 'CONTENDER_EXECUTION': 'later-contender'}
        for field in ('LEASE_PHASE_STARTED_AT', 'OWNER_RUN_ID', 'CONTENDER_RUN_ID'):
            retry_env.pop(field)
        result = subprocess.run(command, env=retry_env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertEqual(path.read_bytes(), original)



    def test_followup_unsigned_preset_flags_refuse_before_proof(self):
        result = self.run_phase('00-preflight.sh', PMAX_SIGNED_REVIEW='', CONFIG_FILE='',
            CONFIG_LOCAL=str(self.work/'validated.yaml'), PMAX_ALERT_CONFIRMED='1',
            PMAX_SKIPPED_ALERT_SILENT='1')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('alert confirmation flags require an existing alert-proof', result.stdout)

    def test_followup_completed_without_failed_tasks_cannot_prove_alert(self):
        self.lease_proof()
        result = self.run_phase('90-alert.sh', U11_FAULT='alert_no_failed_tasks',
            PMAX_NOTIFICATION_CHANNEL='test-channel')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('positive failed-task count', result.stdout)
        self.assertFalse((self.work/'deployments'/self.env['PROJECT']/'alert-proof-sha256-current.json').exists())



    def seed_continuation(self, directory=None, upgrade=True, pass_number=2):
        from datetime import datetime, timezone
        directory = directory or self.work
        records = directory / 'deployments' / self.env['PROJECT']
        records.mkdir(parents=True, exist_ok=True)
        (records / 'ladder-continuation.json').write_text(json.dumps({
            'version':1, 'project':self.env['PROJECT'], 'region':self.env['REGION'],
            'generation':'f'*32, 'repository_head_commit':'a'*40, 'pass_number':pass_number,
            'image_ref':self.env['IMAGE_REF'], 'upgrade':upgrade,
            'baselines':({'1':f"observation-before-sha256-current-{'f' * 32}-pass1.json",
                          '2':f"observation-before-sha256-current-{'f' * 32}-pass2.json"} if upgrade else {}),
            'started_at':datetime.now(timezone.utc).isoformat()}))

    def test_r2_missing_pass1_is_named_refusal_without_active_fallback(self):
        self.alert_proof()
        directory = self.work / 'deployments' / self.env['PROJECT']
        active = [{'row_count':'1', 'observed_days':'1', 'latest_observed_day':'2026-09-01'}]
        (directory / 'observation-before.json').write_text(json.dumps(active))
        (directory / f"observation-before-sha256-current-{'f' * 32}-pass2.json").write_text(json.dumps(active))
        result = self.run_phase('95-resume.sh', ALERT_PROVEN='1', PMAX_ALERT_CONFIRMED='1',
            PMAX_SKIPPED_ALERT_SILENT='1', OBSERVATION_SQL='SELECT row_count FROM raw_observations')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('first-pass observation baseline is missing', result.stdout)
        self.assertNotIn('Traceback', result.stdout)
        self.assertFalse(any(c['args'][:3] == ['scheduler','jobs','resume'] for c in self.calls()))

    def test_r2_pass2_missing_skipped_record_refuses_before_query(self):
        command = ['bash', '-c', 'set -euo pipefail; source "$1"; record_pass1_skipped_execution',
                   'bash', str(self.functions)]
        env = {**self.env, 'LADDER_GENERATION':'f'*32, 'LADDER_PASS_NUMBER':'2', 'PMAX_ENV':'prod',
               'LEASE_PHASE_STARTED_AT':'2026-09-18T00:00:00Z',
               'OWNER_RUN_ID':'ldo-fixture', 'OWNER_EXECUTION':'owner-fixture',
               'CONTENDER_RUN_ID':'ldc-fixture', 'CONTENDER_EXECUTION':'contender-fixture'}
        result = subprocess.run(command, env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertIn('pass-1 SKIPPED execution evidence is missing', result.stderr)
        self.assertFalse(any(c['command']=='bq' for c in self.calls()))
        self.assertFalse((self.work/'deployments'/self.env['PROJECT']/'lease-drill-evidence-sha256-current-pass1.json').exists())

    def test_r2_authorized_user_with_impersonation_url_refuses(self):
        self.adc.write_text(json.dumps({'type':'authorized_user',
            'service_account_impersonation_url':'https://example.test/impersonate'}))
        result = self.run_phase('68-migration.sh')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(any(c['command'] in ('bq','retention_sql','adc_tokeninfo') for c in self.calls()))

    def test_r2_alert_proof_requires_matching_source_records(self):
        directory = self.work / 'deployments' / self.env['PROJECT']
        for filename, field, value in (
            ('alert-submission-sha256-current.json','failed_execution_id','other-execution'),
            ('alert-submission-sha256-current.json','submitted_at','2026-09-18T00:50:00+00:00'),
            ('lease-drill-evidence-sha256-current-pass1.json','skipped_execution_id','other-skip'),
            ('lease-drill-evidence-sha256-current-pass1.json','skipped_run_id','other-run'),
            ('lease-drill-evidence-sha256-current-pass1.json','skipped_observed_at','2026-09-17T00:00:00+00:00'),
            ('alert-submission-sha256-current.json','image_digest','other-image'),
            ('lease-drill-evidence-sha256-current-pass1.json',None,None)):
            with self.subTest(filename=filename, field=field):
                self.alert_proof()
                target=directory/filename
                if field:
                    document=json.loads(target.read_text()); document[field]=value
                    target.write_text(json.dumps(document))
                else: target.unlink()
                self.log.write_text('')
                result = self.run_phase('95-resume.sh', UPGRADE='0', ALERT_PROVEN='1',
                    PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn('alert-proof', result.stdout)
                self.assertFalse(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))

    def test_r2_phase68_does_not_invent_adc_verification(self):
        source=self.functions.read_text()
        source=re.sub(r'^capture_retention_operator\(\) \{\n.*?^\}',
            'capture_retention_operator() {\n  RETENTION_OPERATOR_ACCOUNT=operator@example.test\n  RETENTION_OPERATOR_IMPERSONATION=\n  RETENTION_OPERATOR_ADC_VERIFIED=false\n}',
            source, flags=re.M|re.S)
        self.functions.write_text(source)
        result=self.run_phase('68-migration.sh')
        path=self.work/'deployments'/self.env['PROJECT']/'migration-sha256-current.json'
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(path.exists(), 'phase68 must not claim unverified ADC as true')

    def run_pty(self, command, env, answer=None):
        import errno
        import pty
        import select
        import time
        master, slave = pty.openpty()
        process = subprocess.Popen(command, env=env, stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        output = bytearray()
        sent = False
        deadline = time.monotonic() + 20
        try:
            while time.monotonic() < deadline:
                ready, _, _ = select.select([master], [], [], 0.1)
                if ready:
                    try: part=os.read(master, 65536)
                    except OSError as exc:
                        if exc.errno == errno.EIO: break
                        raise
                    if not part: break
                    output.extend(part)
                    if answer is not None and not sent and b'Type yes:' in output:
                        os.write(master, (answer+'\n').encode()); sent=True
                if process.poll() is not None and not ready: break
            if process.poll() is None:
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    self.fail('pty command timed out: '+output.decode(errors='replace'))
            return subprocess.CompletedProcess(command, process.wait(), output.decode(errors='replace'))
        finally:
            os.close(master)
            if process.poll() is None: process.kill(); process.wait()

    def test_r2_tty_resume_prompt_requires_yes_and_preserves_flags_on_no(self):
        self.alert_proof()
        for answer in ('no', 'yes'):
            with self.subTest(answer=answer):
                script = '''set -euo pipefail; source "$1"
trap 'printf "FLAGS=%s/%s\\n" "${PMAX_ALERT_CONFIRMED:-unset}" "${PMAX_SKIPPED_ALERT_SILENT:-unset}"' EXIT
confirm_human_phase 95-resume
'''
                result=self.run_pty(['bash','-c',script,'bash',str(self.functions)],
                    {**self.env,'LADDER_GENERATION':'f'*32,'PMAX_CONFIRMED_PHASES':'','PMAX_ALERT_CONFIRMED':'',
                     'PMAX_SKIPPED_ALERT_SILENT':''}, answer)
                if answer=='no':
                    self.assertNotEqual(result.returncode,0,result.stdout)
                    self.assertIn('FLAGS=unset/unset',result.stdout)
                else:
                    self.assertEqual(result.returncode,0,result.stdout)
                    self.assertIn('FLAGS=1/1',result.stdout)

    def test_r2_listed_resume_tty_preflight_requires_flags(self):
        inputs={**self.signed_inputs(), 'PMAX_ALERT_CONFIRMED':'', 'PMAX_SKIPPED_ALERT_SILENT':'',
                'PMAX_CONFIRMED_PHASES':'68-migration,89-retention,95-resume'}
        result=self.run_pty(['bash','-c','set -euo pipefail; source "$1"; source "$2"',
            'bash',str(self.functions),str(root/'deploy/phases/00-preflight.sh')],{**self.env,**inputs})
        self.assertNotEqual(result.returncode,0,result.stdout)
        self.assertIn('PMAX_ALERT_CONFIRMED=1',result.stdout)
        inputs.update(PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
        result=self.run_pty(['bash','-c','set -euo pipefail; source "$1"; source "$2"',
            'bash',str(self.functions),str(root/'deploy/phases/00-preflight.sh')],{**self.env,**inputs})
        self.assertEqual(result.returncode,0,result.stdout)

    def test_r2_flags_without_listed_resume_or_proof_refuse_before_mutation(self):
        result=self.run_phase('00-preflight.sh', PMAX_SIGNED_REVIEW='', CONFIG_FILE='',
            CONFIG_LOCAL=str(self.work/'validated.yaml'), PMAX_ALERT_CONFIRMED='1',
            PMAX_SKIPPED_ALERT_SILENT='1', PMAX_CONFIRMED_PHASES='68-migration,89-retention')
        self.assertNotEqual(result.returncode,0,result.stdout)
        self.assertFalse(any(c['command']=='bq' or c['args'][:3]==['run','jobs','execute']
                             for c in self.calls()))

    def make_followup_driver(self, *, real25=False, proof=False, signed=False):
        ladder=self.work/'driver'
        phases=ladder/'deploy/phases'
        phases.mkdir(parents=True)
        source=(root/'deploy/deploy.sh').read_text()
        driver=ladder/'deploy/deploy.sh'; driver.write_text(source)
        (ladder/'deploy/alert-policy.json').symlink_to(root/'deploy/alert-policy.json')
        specs=re.findall(r'^  "([0-9]+-[^|]+)\|[^\"]+"$',source,re.M)
        for phase in specs: (phases/(phase+'.sh')).write_text(':\n')
        (phases/'00-preflight.sh').write_text('UPGRADE=1; export UPGRADE\nsource "$U11_SOURCE_ROOT/deploy/phases/00-preflight.sh"\n')
        if real25:
            (phases/'25-dry-run.sh').write_text('source "$U11_SOURCE_ROOT/deploy/phases/25-dry-run.sh"\n')
        (phases/'50-build-deploy.sh').write_text('IMAGE_REF="$PMAX_IMAGE_REF"; export IMAGE_REF\n')
        (phases/'75-lease-drill.sh').write_text('''LEASE_PHASE_STARTED_AT=2026-09-18T00:00:00Z
OWNER_RUN_ID=ldo-fixture
OWNER_EXECUTION=owner-fixture
CONTENDER_RUN_ID=ldc-fixture
CONTENDER_EXECUTION=contender-fixture
''')
        (phases/'90-alert.sh').write_text('source "$U11_SOURCE_ROOT/deploy/phases/90-alert.sh"\n')
        (phases/'execution-poll.sh').symlink_to(root/'deploy/phases/execution-poll.sh')
        (phases/'95-resume.sh').write_text('source "$U11_SOURCE_ROOT/deploy/phases/95-resume.sh"\n')
        records=ladder/'deployments'/self.env['PROJECT']; records.mkdir(parents=True)
        self.seed_continuation(ladder, upgrade=False, pass_number=1 if not signed else 2)
        if proof:
            self.alert_proof()
            for name in ('alert-proof-sha256-current.json','alert-submission-sha256-current.json',
                         'lease-drill-evidence-sha256-current-pass1.json'):
                shutil.copy2(self.work/'deployments'/self.env['PROJECT']/name,records/name)
        elif signed:
            shutil.copy2(self.lease_proof(),records/'lease-drill-evidence-sha256-current-pass1.json')
        env={**self.env,'PMAX_IMAGE_REF':self.env['IMAGE_REF'],
            'PMAX_SIGNED_REVIEW':str(self.signed) if signed else '',
            'PMAX_NOTIFICATION_CHANNEL':'test-channel', 'PMAX_ALERT_CONFIRMED':'1' if proof else '',
            'PMAX_SKIPPED_ALERT_SILENT':'1' if proof else '',
            'PMAX_CONFIRMED_PHASES':','.join(p for p in specs if p not in ('85-review','95-resume')),
            'REVIEW_RECORDED':'1'}
        command=['bash',str(driver),'--project',self.env['PROJECT'],'--region',self.env['REGION'],
                 '--config-uri',self.env['CONFIG_URI'],'--credential-file',self.env['CREDENTIAL_FILE']]
        return command,env,records

    def check_alert_driver_work_directory_cleanup(self, *, refused):
        command, env, records = self.make_followup_driver(real25=True, signed=True)
        temporary_root = self.work / 'deploy-tmp'
        temporary_root.mkdir()
        env['TMPDIR'] = str(temporary_root)
        work_record = self.work / 'driver-work-dir.txt'
        env['ALERT_TEST_WORK_RECORD'] = str(work_record)
        phase90 = records.parents[1] / 'deploy/phases/90-alert.sh'
        phase90.write_text('''printf '%s\\n' "$WORK_DIR" >"$ALERT_TEST_WORK_RECORD"
source "$U11_SOURCE_ROOT/deploy/phases/90-alert.sh"
PMAX_ALERT_CONFIRMED=1
PMAX_SKIPPED_ALERT_SILENT=1
PMAX_CONFIRMED_PHASES+=,95-resume
export PMAX_ALERT_CONFIRMED PMAX_SKIPPED_ALERT_SILENT PMAX_CONFIRMED_PHASES
''')
        if refused:
            skipped = records / 'lease-drill-evidence-sha256-current-pass1.json'
            value = json.loads(skipped.read_text())
            value['skipped_observed_at'] = '2099-01-01T00:00:00+00:00'
            skipped.write_text(json.dumps(value))
        result = subprocess.run(command, env=env, text=True, capture_output=True,
                                stdin=subprocess.DEVNULL)
        output = result.stdout + result.stderr
        self.assertIn('PHASE 90-alert', output)
        proof = records / 'alert-proof-sha256-current.json'
        if refused:
            self.assertNotEqual(result.returncode, 0, output)
            self.assertIn('alert-proof is invalid', output)
            self.assertFalse(proof.exists())
        else:
            self.assertEqual(result.returncode, 0, output)
            self.assertIn('phase 90: alert-proof recorded', output)
            self.assertEqual(proof.stat().st_mode & 0o777, 0o600)
        self.assertFalse([p.name for p in records.iterdir() if p.name.startswith('.')],
                         'temporary record left behind')
        work_dir = Path(work_record.read_text().strip())
        self.assertEqual(work_dir.parent, temporary_root)
        self.assertFalse(work_dir.exists(), 'phase 90 left WORK_DIR behind at driver exit')

    def test_alert_driver_cleans_work_directory_after_success(self):
        self.check_alert_driver_work_directory_cleanup(refused=False)

    def test_alert_driver_cleans_work_directory_after_refused_proof(self):
        self.check_alert_driver_work_directory_cleanup(refused=True)

    def plan_driver_result(self, state_kind):
        command, env, records = self.make_followup_driver(real25=True, proof=True, signed=True)
        self.assertNotIn('LADDER_GENERATION', env)
        env['PMAX_CONFIRMED_PHASES'] += ',95-resume'
        if state_kind == 'absent':
            for path in records.iterdir():
                path.unlink()
            self.anchor_record()
            shutil.copy2(self.work/'deployments'/self.env['PROJECT']/'rollback-anchor.txt',
                         records/'rollback-anchor.txt')
        elif state_kind == 'foreign':
            proof = records/'alert-proof-sha256-current.json'
            value = json.loads(proof.read_text())
            value['generation'] = 'a'*32
            proof.write_text(json.dumps(value))
        phase00 = records.parents[1]/'deploy/phases/00-preflight.sh'
        phase00.write_text('printf "ENTRY_GENERATION=%s\\n" "${LADDER_GENERATION:-unset}"\n'
                           + phase00.read_text())
        before = {path.name: (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_mode)
                  for path in records.iterdir()}
        result = subprocess.run(command + ['--plan'], env=env, text=True,
                                capture_output=True, stdin=subprocess.DEVNULL)
        after = {path.name: (path.read_bytes(), path.stat().st_mtime_ns, path.stat().st_mode)
                 for path in records.iterdir()}
        self.assertEqual(after, before, 'plan must not prepare or write ladder evidence')
        self.assertFalse(any(c['args'][:3] in (['run','jobs','execute'], ['scheduler','jobs','resume'])
                             for c in self.calls()))
        return result.returncode, result.stdout + result.stderr

    def test_r4_plan_driver_accepts_active_generation_proof_readonly(self):
        code, output = self.plan_driver_result('active')
        self.assertEqual(code, 0, output)
        self.assertIn('ENTRY_GENERATION=' + 'f'*32, output)
        self.assertIn('PHASE 95-resume', output)
        self.assertNotIn('plan: no active ladder generation', output)

    def test_r4_plan_driver_without_generation_defers_alert_validation(self):
        code, output = self.plan_driver_result('absent')
        self.assertEqual(code, 0, output)
        self.assertIn('ENTRY_GENERATION=NONE', output)
        self.assertIn('plan: no active ladder generation; alert-proof validation runs on the live pass', output)
        self.assertIn('PHASE 95-resume', output)

    def test_r4_plan_driver_refuses_foreign_generation_proof(self):
        code, output = self.plan_driver_result('foreign')
        self.assertNotEqual(code, 0, output)
        self.assertIn('alert-proof is invalid for this digest, generation or its timestamps', output)
        self.assertNotIn('PHASE 10-apis', output)

    def test_alert_preflight_conditional_caller_stops_driver_on_bad_proof(self):
        command, env, records = self.make_followup_driver(real25=True, proof=True, signed=True)
        env['PMAX_CONFIRMED_PHASES'] += ',95-resume'
        proof = records / 'alert-proof-sha256-current.json'
        value = json.loads(proof.read_text())
        value['generation'] = 'a' * 32
        proof.write_text(json.dumps(value))
        preflight = (root / 'deploy/phases/00-preflight.sh').read_text()
        self.assertEqual(preflight.count('\nvalidate_alert_preflight\n'), 1)
        preflight = preflight.replace('\nvalidate_alert_preflight\n',
                                      '\nif validate_alert_preflight; then echo unexpected; fi\n')
        (records.parents[1] / 'deploy/phases/00-preflight.sh').write_text(
            'UPGRADE=1; export UPGRADE\n' + preflight)
        result = subprocess.run(command + ['--plan'], env=env, text=True,
                                capture_output=True, stdin=subprocess.DEVNULL)
        output = result.stdout + result.stderr
        self.assertIn('alert-proof is invalid for this digest, generation or its timestamps', output)
        self.assertNotEqual(result.returncode, 0, output)
        self.assertNotIn('unexpected', output)
        self.assertNotIn('PHASE 10-apis', output)

    def test_r5_alert_archive_accepts_identical_retry_and_refuses_different_bytes(self):
        names = ('alert-proof-sha256-current.json', 'alert-submission-sha256-current.json',
                 'lease-drill-evidence-sha256-current-pass1.json')
        for name in names:
            with self.subTest(name=name):
                self.alert_proof()
                records = self.work/'deployments'/self.env['PROJECT']
                state_path = records/'ladder-continuation.json'
                state = json.loads(state_path.read_text())
                state['generation'] = 'a'*32
                state['archived_alert_records'] = []
                state_path.write_text(json.dumps(state))
                record = records/name
                original = record.read_bytes()
                archive = record.with_name(record.stem + '-' + 'f'*32 + '.json')
                # Simulate an interruption after archive creation, before state save.
                archive.write_bytes(original)
                archive.chmod(0o600)
                command = ['bash', '-c', 'set -euo pipefail; source "$1"; archive_alert_record "$2"',
                           'bash', str(self.functions), str(record)]
                env = {**self.env, 'LADDER_GENERATION': 'a'*32}
                result = subprocess.run(command, env=env, text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(archive.read_bytes(), original)
                self.assertEqual(archive.stat().st_mode & 0o777, 0o600)
                self.assertEqual(json.loads(state_path.read_text())['archived_alert_records'], [str(archive)])
                state_before = state_path.read_bytes()
                result = subprocess.run(command, env=env, text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(state_path.read_bytes(), state_before)
                archive.write_bytes(original + b'\n')
                result = subprocess.run(command, env=env, text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('refusing to overwrite alert evidence archive: ' + archive.name,
                              result.stdout + result.stderr)
                self.assertEqual(archive.read_bytes(), original + b'\n')
                self.assertEqual(record.read_bytes(), original)
                self.assertEqual(state_path.read_bytes(), state_before)

    def check_refused_alert_writer_retry(self, kind, fault):
        proof = self.alert_proof()
        records = proof.parent
        state_path = records/'ladder-continuation.json'
        state = json.loads(state_path.read_text())
        state.update(generation='a'*32, pass_number=1, upgrade=False, baselines={})
        state_path.write_text(json.dumps(state))
        skipped = records/'lease-drill-evidence-sha256-current-pass1.json'
        submission = records/'alert-submission-sha256-current.json'
        paths = {'skipped': skipped, 'submission': submission, 'proof': proof}
        for source in (() if kind == 'skipped' else (skipped,) if kind == 'submission' else (skipped, submission)):
            value = json.loads(source.read_text())
            value['generation'] = 'a'*32
            source.write_text(json.dumps(value))
        current = paths[kind]
        current.chmod(0o644)  # a replaced record must come back at 0600 through the rename, never an in-place write
        original = current.read_bytes()
        archive = current.with_name(current.stem + '-' + 'f'*32 + '.json')
        state_before = state_path.read_bytes()
        inputs = {'LADDER_GENERATION': 'a'*32, 'LADDER_PASS_NUMBER': '1',
                  'PMAX_ENV': 'prod', 'PMAX_NOTIFICATION_CHANNEL': 'test-channel',
                  'PMAX_ALERT_CONFIRMED': '', 'PMAX_SKIPPED_ALERT_SILENT': '',
                  'LEASE_PHASE_STARTED_AT': '2026-09-18T00:00:00Z',
                  'OWNER_RUN_ID': 'ldo-fixture', 'OWNER_EXECUTION': 'owner-fixture',
                  'CONTENDER_RUN_ID': 'ldc-fixture', 'CONTENDER_EXECUTION': 'contender-fixture'}
        def run(**changes):
            if kind != 'skipped':
                return self.run_phase('90-alert.sh', **{**inputs, **changes})
            return subprocess.run(['bash', '-c',
                'set -euo pipefail; source "$1"; record_pass1_skipped_execution',
                'bash', str(self.functions)], env={**self.env, **inputs, **changes},
                stdin=subprocess.DEVNULL, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        source_before = None
        if fault in ('skipped_identity', 'proof_timestamp'):
            source = skipped if fault == 'skipped_identity' else submission
            source_before = source.read_bytes()
            value = json.loads(source.read_text())
            value['skipped_execution_id' if fault == 'skipped_identity' else 'submitted_at'] = ''
            source.write_text(json.dumps(value))
        changes = ({'OWNER_RUN_ID': 'fixture', 'CONTENDER_RUN_ID': 'fixture'}
                   if fault == 'ambiguous_execution' else {'U11_FAULT': fault})
        result = run(**changes)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        needles = {'skipped_status': 'requires one SUCCESS and one SKIPPED',
                   'ambiguous_execution': 'does not bind exactly one drill execution',
                   'submission_write': 'injected submission staging failure',
                   'skipped_identity': 'pass-1 SKIPPED execution evidence is invalid',
                   'proof_timestamp': 'alert-proof is invalid'}
        self.assertIn(needles[fault], result.stdout)
        self.assertEqual(current.read_bytes(), original, 'refused writer changed current record')
        self.assertFalse(archive.exists(), 'refused writer archived the prior generation')
        self.assertEqual(state_path.read_bytes(), state_before)
        if source_before is not None:
            source.write_bytes(source_before)
        result = run()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(current.read_text())['generation'], 'a'*32)
        self.assertEqual(archive.read_bytes(), original)
        self.assertEqual(archive.stat().st_mode & 0o777, 0o600)
        self.assertEqual(current.stat().st_mode & 0o777, 0o600, 'replaced record was written in place, not renamed')
        self.assertFalse([p.name for p in records.iterdir() if p.name.startswith('.')], 'temporary record left behind')
        self.assertEqual(json.loads(state_path.read_text())['archived_alert_records'].count(str(archive)), 1)
        completed = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (current, archive, state_path)}
        self.log.write_text('')
        result = run()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual({p: (p.read_bytes(), p.stat().st_mtime_ns) for p in completed}, completed)
        self.assertFalse(any(c['command'] in ('gcloud', 'bq') for c in self.calls()))

    def test_r5_malformed_archived_alert_records_refuses_by_name(self):
        proof = self.alert_proof()
        records = proof.parent
        state_path = records/'ladder-continuation.json'
        for bad in ('not-a-list', {'a': 1}, [1], ['']):
            with self.subTest(bad=bad):
                state = json.loads(state_path.read_text())
                state['generation'] = 'a'*32
                state['archived_alert_records'] = bad
                state_path.write_text(json.dumps(state))
                archive = proof.with_name(proof.stem + '-' + 'f'*32 + '.json')
                archive.unlink(missing_ok=True)
                result = subprocess.run(['bash', '-c', 'set -euo pipefail; source "$1"; archive_alert_record "$2"',
                                         'bash', str(self.functions), str(proof)],
                                        env={**self.env, 'LADDER_GENERATION': 'a'*32}, text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn('ladder continuation state is malformed', result.stdout + result.stderr)
                self.assertFalse(archive.exists(), 'malformed state still produced an archive')

    def test_r5_skipped_status_refusal_preserves_current_and_retry_succeeds(self):
        self.check_refused_alert_writer_retry('skipped', 'skipped_status')

    def test_r5_skipped_execution_refusal_preserves_current_and_retry_succeeds(self):
        self.check_refused_alert_writer_retry('skipped', 'ambiguous_execution')

    def test_r5_submission_write_refusal_preserves_current_and_retry_succeeds(self):
        self.check_refused_alert_writer_retry('submission', 'submission_write')

    def test_r5_proof_source_refusal_preserves_current_and_retry_succeeds(self):
        self.check_refused_alert_writer_retry('proof', 'skipped_identity')

    def test_r5_proof_timestamp_refusal_preserves_current_and_retry_succeeds(self):
        self.check_refused_alert_writer_retry('proof', 'proof_timestamp')

    def test_r3_alert_proof_refuses_each_foreign_generation(self):
        directory = self.work/'deployments'/self.env['PROJECT']
        for filename in ('alert-proof-sha256-current.json', 'alert-submission-sha256-current.json',
                         'lease-drill-evidence-sha256-current-pass1.json'):
            for generation in ('a'*32, None):
                with self.subTest(filename=filename, generation=generation):
                    self.alert_proof()
                    target = directory/filename
                    value = json.loads(target.read_text()); value['generation'] = generation
                    target.write_text(json.dumps(value))
                    self.log.write_text('')
                    result = self.run_phase('95-resume.sh', UPGRADE='0', ALERT_PROVEN='1',
                        PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn('alert-proof', result.stdout)
                    self.assertFalse(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))

    def test_r3_phase90_refuses_foreign_generation_skipped_record(self):
        path = self.lease_proof()
        value = json.loads(path.read_text()); value['generation'] = 'a'*32
        path.write_text(json.dumps(value))
        result = self.run_phase('90-alert.sh', PMAX_NOTIFICATION_CHANNEL='test-channel')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('pass-1 SKIPPED', result.stdout)
        self.assertFalse(any(c['args'][:3]==['run','jobs','execute'] for c in self.calls()))
        self.assertFalse(any(c['args'][:3]==['alpha','monitoring','policies'] for c in self.calls()))

    def test_r3_completed_first_deploy_same_image_upgrade_refreshes_alert_drill(self):
        command,env,records = self.make_followup_driver(real25=True,proof=True,signed=True)
        env['PMAX_CONFIRMED_PHASES'] += ',95-resume'
        result = subprocess.run(command, env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        names = ('alert-proof-sha256-current.json', 'alert-submission-sha256-current.json',
                 'lease-drill-evidence-sha256-current-pass1.json')
        old_bytes = {name: (records/name).read_bytes() for name in names}
        old = {name: json.loads(old_bytes[name]) for name in names}
        old_generation = json.loads((records/'ladder-continuation.json').read_text())['generation']
        self.assertEqual(json.loads((records/'resume-evidence-sha256-current.json').read_text())['generation'], old_generation)
        self.anchor_record()
        shutil.copy2(self.work/'deployments'/self.env['PROJECT']/'rollback-anchor.txt', records/'rollback-anchor.txt')
        env.update(PMAX_SIGNED_REVIEW='', PMAX_ALERT_CONFIRMED='', PMAX_SKIPPED_ALERT_SILENT='')
        env['PMAX_CONFIRMED_PHASES'] = env['PMAX_CONFIRMED_PHASES'].replace(',95-resume','')
        self.log.write_text('')
        result = subprocess.run(command, env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        output = result.stdout+result.stderr
        self.assertNotEqual(result.returncode, 0, output)
        self.assertIn('human-owned phase 95-resume needs the operator', output)
        generation = json.loads((records/'ladder-continuation.json').read_text())['generation']
        self.assertNotEqual(generation, old_generation)
        for name in names:
            value = json.loads((records/name).read_text())
            self.assertEqual(value.get('generation'), generation, name)
            self.assertNotEqual(value, old[name], name)
            archive = records/(Path(name).stem + '-' + old_generation + '.json')
            self.assertTrue(archive.is_file(), 'prior alert evidence must be archived: ' + name)
            self.assertEqual(archive.read_bytes(), old_bytes[name])
            self.assertEqual(archive.stat().st_mode & 0o777, 0o600)
            self.assertIn(str(archive), json.loads((records/'ladder-continuation.json').read_text())
                          ['archived_alert_records'])
        self.assertEqual(sum(c['args'][:3]==['run','jobs','execute'] for c in self.calls()), 1)
        self.assertEqual(sum(c['args'][:4]==['alpha','monitoring','policies','update'] for c in self.calls()), 1)
        self.assertTrue(any('SELECT run_id, status' in ' '.join(c['args']) for c in self.calls()))
        self.assertFalse(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))
        fresh = {name: (records/name).read_bytes() for name in names}
        env.update(PMAX_SIGNED_REVIEW=str(self.signed), PMAX_ALERT_CONFIRMED='1', PMAX_SKIPPED_ALERT_SILENT='1')
        env['PMAX_CONFIRMED_PHASES'] += ',95-resume'
        result = subprocess.run(command, env=env, text=True, capture_output=True, stdin=subprocess.DEVNULL)
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertEqual(json.loads((records/'resume-evidence-sha256-current.json').read_text())['generation'], generation)
        self.assertTrue(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))
        self.assertEqual(sum(c['args'][:3]==['run','jobs','execute'] for c in self.calls()), 1)
        for name in names: self.assertEqual((records/name).read_bytes(), fresh[name])

    def test_r2_driver_keeps_real_pass75_evidence_hook(self):
        command,env,records=self.make_followup_driver()
        result=subprocess.run(command,env=env,text=True,capture_output=True,stdin=subprocess.DEVNULL)
        combined=result.stdout+result.stderr
        self.assertNotEqual(result.returncode,0,combined)
        self.assertIn('human-owned phase 95-resume needs the operator',combined)
        path=records/'lease-drill-evidence-sha256-current-pass1.json'
        self.assertTrue(path.exists(),'driver must call real pass75 evidence helper')
        self.assertEqual(json.loads(path.read_text())['skipped_execution_id'],'contender-fixture')
        self.assertTrue((records/'alert-proof-sha256-current.json').exists())
        self.assertFalse(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))

    def test_r2_driver_signed_no_flags_stops_at_human95(self):
        command,env,records=self.make_followup_driver(signed=True)
        result=subprocess.run(command,env=env,text=True,capture_output=True,stdin=subprocess.DEVNULL)
        combined=result.stdout+result.stderr
        self.assertNotEqual(result.returncode,0,combined)
        self.assertIn('PHASE 90-alert',combined)
        self.assertIn('human-owned phase 95-resume needs the operator',combined)
        self.assertTrue((records/'alert-proof-sha256-current.json').exists())
        self.assertFalse(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))

    def test_r2_driver_listed95_without_flags_refuses_in00(self):
        command,env,records=self.make_followup_driver(signed=True)
        env['PMAX_CONFIRMED_PHASES']+=',95-resume'
        result=subprocess.run(command,env=env,text=True,capture_output=True,stdin=subprocess.DEVNULL)
        combined=result.stdout+result.stderr
        self.assertNotEqual(result.returncode,0,combined)
        self.assertIn('requires PMAX_ALERT_CONFIRMED=1',combined)
        self.assertNotIn('PHASE 90-alert',combined)
        self.assertFalse((records/'alert-proof-sha256-current.json').exists())

    def test_r2_first_deployment_signed_pass2_resumes_without_upgrade_floor(self):
        command,env,records=self.make_followup_driver(real25=True,proof=True,signed=True)
        env['PMAX_CONFIRMED_PHASES']+=',95-resume'
        result=subprocess.run(command,env=env,text=True,capture_output=True,stdin=subprocess.DEVNULL)
        combined=result.stdout+result.stderr
        self.assertEqual(result.returncode,0,combined)
        self.assertIn('PHASE 95-resume',combined)
        self.assertTrue(any(c['args'][:3]==['scheduler','jobs','resume'] for c in self.calls()))
        self.assertFalse(any('raw_observations' in ' '.join(c['args']) for c in self.calls()))
        self.assertFalse(list(records.glob('observation-before*')))
        self.assertTrue(json.loads((records/'resume-evidence-sha256-current.json').read_text())['resumed'])


    def test_r2_unsigned_listed95_requires_flags_in00(self):
        result=self.run_phase('00-preflight.sh', PMAX_SIGNED_REVIEW='', CONFIG_FILE='',
            CONFIG_LOCAL=str(self.work/'validated.yaml'), PMAX_ALERT_CONFIRMED='',
            PMAX_SKIPPED_ALERT_SILENT='', PMAX_CONFIRMED_PHASES='68-migration,89-retention,95-resume')
        self.assertNotEqual(result.returncode,0,result.stdout)
        self.assertIn('PMAX_ALERT_CONFIRMED=1',result.stdout)


    def test_r2_unlisted_unsigned_unpinned_preflight_needs_no_alert_proof(self):
        for plan in ('0','1'):
            with self.subTest(plan=plan):
                result=self.run_phase('00-preflight.sh', PLAN=plan, UPGRADE='0',
                    IMAGE_REF='', PMAX_IMAGE_REF='', PMAX_SIGNED_REVIEW='',
                    PMAX_ALERT_CONFIRMED='', PMAX_SKIPPED_ALERT_SILENT='',
                    PMAX_CONFIRMED_PHASES='68-migration,89-retention', CONFIG_FILE=str(self.config),
                    CONFIG_LOCAL=str(self.work/'validated.yaml'))
                self.assertEqual(result.returncode,0,result.stdout)

unittest.main(argv=['u11', *(shlex.split(os.environ['U11_CASE']) if os.environ.get('U11_CASE') else [])], verbosity=2)
PY_U11
fi
[[ "${U11_TESTS_ONLY:-0}" != 1 ]] || exit 0

# Shared production helpers for legacy phase-only harness calls.
uv run python - "$DEPLOY" "$TMP/ladder-functions.sh" <<'PY_SHARED_FUNCTIONS'
from pathlib import Path
import re
import sys
source = Path(sys.argv[1]).read_text()
Path(sys.argv[2]).write_text("\n".join(re.findall(r'^\w+\(\) \{\n.*?^\}', source, re.M | re.S)))
PY_SHARED_FUNCTIONS
export BASH_ENV="$TMP/ladder-functions.sh"

# Legacy isolated product roots have no checkout; model the read-only HEAD boundary.
cat >"$TMP/bin/git" <<'SH_LADDER_GIT'
#!/usr/bin/env bash
if [[ "$#" == 4 && "$1" == -C && "$3" == rev-parse && "$4" == HEAD ]]; then
  printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
else
  echo "unsupported git fixture leaf" >&2
  exit 2
fi
SH_LADDER_GIT
chmod +x "$TMP/bin/git"

# Behavioural tests run with isolated CLI shims, never cloud credentials.
uv run python - "$ROOT" "$TMP" <<'PY_U18'
from __future__ import annotations

import contextlib
import importlib.util
import io
import hashlib
import re
import shlex
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import yaml
from pmax_pack.config import load_config

root = Path(os.environ.get('U18_SOURCE_ROOT', sys.argv[1]))
base = Path(sys.argv[2])


class LookerDeployTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.directory.cleanup)
        self.work = Path(self.directory.name)
        self.bin = self.work / 'bin'
        self.bin.mkdir()
        self.log = self.work / 'calls.jsonl'
        self.config = self.work / 'config.yaml'
        self.config.write_text((base / 'config.yaml').read_text() + '''
env: verify
editors: ["user:editor@example.test", "user:second@example.test"]
looker_service_agents: ["serviceAccount:agent-a@example.test", "serviceAccount:agent-b@example.test"]
''')
        self.env = {
            **os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}',
            'U18_REAL_UV': shutil.which('uv'), 'U18_LOG': str(self.log), 'ROOT': str(root), 'PLAN': '0',
            'PROJECT': 'test-pmax-project', 'REGION': 'europe-west1',
            'WORK_DIR': str(self.work / 'evidence'), 'CONFIG_LOCAL': str(self.config),
            'CONFIG_FILE': str(self.config), 'CONFIG_URI': 'gs://test-config-bucket/test.yaml',
            'PHASE_STATE': str(self.work / 'phase.env'), 'UPGRADE': '0',
            'CREDENTIAL_FILE': str(base / 'credential.yaml'),
            'OPERATOR_IDENTITY': 'operator@example.test', 'SECRET_NAME': 'pmax-google-ads',
            'PMAX_NOTIFICATION_CHANNEL': 'test-channel', 'PMAX_ENV': 'verify',
            'PMAX_LOOKER_PROBE_EXPIRES_AT': '2099-01-01T00:00:00Z',
            'DATASET_REPORTING': 'pmax_reporting', 'DATASET_REPORTING_VERIFY': 'pmax_reporting_verify',
            'DATASETS_CSV': ','.join(dict.fromkeys(vars(load_config(str(self.config)).datasets).values())),
            'REPORT_BUCKET': 'test-report-bucket', 'CONFIG_BUCKET': 'test-config-bucket',
            'IMAGE_REF': 'test-image@sha256:test', 'RUN_DAY': '2026-09-18',
            'RUNTIME_SA': 'pmax-runtime@test-pmax-project.iam.gserviceaccount.com',
            'PMAX_LOOKER_RETRY_SECONDS': '0',
        }
        for suffix, value in [('RAW', 'pmax_raw'), ('MARTS', 'pmax_marts'), ('OPS', 'pmax_ops'),
                              ('SNAPSHOTS', 'pmax_snapshots'), ('PARITY', 'pmax_parity_scratch'),
                              ('PARITY_BQ', 'pmax_parity_scratch_bq'), ('CI', 'pmax_ci_scratch'),
                              ('CI_BQ', 'pmax_ci_scratch_bq'), ('VERIFY', 'pmax_marts_verify')]:
            self.env[f'DATASET_{suffix}'] = value
        Path(self.env['WORK_DIR']).mkdir()
        shim = r'''
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
command = Path(sys.argv[0]).name
impersonated = os.environ.get('CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT', '')
with open(os.environ['U18_LOG'], 'a') as f:
    f.write(json.dumps({'command': command, 'args': args, 'impersonated': impersonated}) + '\n')
text = ' '.join(args)
fault = os.environ.get('U18_FAULT', '')

# Help-validated leaf flags only. Unknown leaves and malformed flags fail closed.
import re

def validate_flags(patterns):
    for arg in args:
        if arg.startswith('-') and not any(re.fullmatch(pattern, arg) for pattern in patterns):
            print('unsupported flag: ' + arg, file=sys.stderr)
            sys.exit(2)

if command == 'bq':
    leaf = next((a for a in args if not a.startswith('-')), '')
    flags = {
        'head': [r'--max_rows=\d+'],
        'ls': [r'--datasets', r'--all', r'--max_results=\d+'],
        'show': [r'--dataset'],
        'query': [r'--use_legacy_sql=false', r'--maximum_bytes_billed=\d+'],
        'update': [r'--dataset', r'--set_label=[a-z_]+:[a-z0-9_]+', r'--default_table_expiration=\d+'],
        'mk': [r'--dataset'],
    }
    if leaf not in flags:
        print('unsupported bq leaf: ' + leaf, file=sys.stderr); sys.exit(2)
    validate_flags(flags[leaf] + [r'--project_id=[a-z0-9-]+', r'--location=EU',
                   r'--format=(json|none)', r'--quiet', r'--request_reason=pmax-looker-(precheck|full)-[a-f0-9]+',
                   r'--use_gcloud_config=true', r'--use_gcloud_config_cache=false'])
if command == 'gcloud':
    common = [r'--project=[a-z0-9-]+', r'--format=(none|json(?:\([\w.]+\))?|value\([\w.]+\))', r'--quiet']
    binding = [r'--member=.+', r'--role=.+', r'--condition=(None|expression=request.time < timestamp\("[0-9TZ:-]+"\),title=pmax-looker-probe-window)']
    role = [r'--title=.+', r'--description=.+', r'--permissions=.+', r'--stage=GA']
    leaves = {
        'projects get-ancestors': [], 'projects describe': [],
        'billing projects describe': [], 'auth list': [r'--filter=status:ACTIVE'],
        'auth print-access-token':
            [r'--impersonate-service-account=.+', r'--lifetime=300'],
        'resource-manager org-policies describe': [r'--effective'],
        'run jobs describe': [r'--region=[a-z0-9-]+'],
        'run jobs execute': [r'--region=[a-z0-9-]+', r'--args=.+', r'--task-timeout=6h', r'--wait'],
        'storage buckets describe': [], 'storage objects describe': [],
        'storage buckets update': [r'--update-labels=app=pmax,env=(prod|verify|parity|ci)', r'--lifecycle-file=.+', r'--public-access-prevention'],
        'iam service-accounts describe': [], 'iam service-accounts create': [r'--display-name=.+'],
        'iam service-accounts get-iam-policy': [],
        'iam service-accounts add-iam-policy-binding': binding,
        'projects add-iam-policy-binding': binding, 'secrets add-iam-policy-binding': binding,
        'storage buckets add-iam-policy-binding': binding,
        'projects get-iam-policy': [], 'projects set-iam-policy': [],
        'iam roles describe': [], 'iam roles create': role, 'iam roles update': role,
        'logging metrics describe': [],
        'logging metrics create': [r'--description=.+', r'--log-filter=.+'],
        'logging metrics update': [r'--description=.+', r'--log-filter=.+'],
        'alpha monitoring policies list': [r'--filter=.+', r'--limit=1'],
        'alpha monitoring policies create': [r'--policy-from-file=.+'],
        'alpha monitoring policies update': [r'--policy-from-file=.+'],
    }
    leaf = next((key for key in leaves if args[:len(key.split())] == key.split()), None)
    if leaf is None:
        print('unsupported gcloud leaf', file=sys.stderr); sys.exit(2)
    validate_flags(common + leaves[leaf])
    if leaf == 'projects get-ancestors' and '--format=value(type)' not in args:
        print('required flag: --format=value(type)', file=sys.stderr); sys.exit(2)
if command == 'uv':
    if any('grant_dataset_access.py' in a for a in args):
        print('user:editor@example.test serviceAccount:agent-a@example.test'); sys.exit(0)
    os.execv(os.environ['U18_REAL_UV'], ['uv', *args])
if command == 'docker':
    print('docker presence shim must never execute', file=sys.stderr); sys.exit(97)
if command == 'sleep':
    sys.exit(0)
if command == 'gcloud':
    if fault == 'iam_failure' and 'add-iam-policy-binding' in args:
        print('private editor@example.test failure', file=sys.stderr); sys.exit(1)
    if args[:3] == ['iam', 'service-accounts', 'get-iam-policy']:
        print(json.dumps({'bindings':[{'role':'roles/iam.serviceAccountTokenCreator','members':['user:operator@example.test'],'condition':{'title':'old-window','expression':'false'}},{'role':'roles/iam.serviceAccountTokenCreator','members':['user:operator@example.test'],'condition':{'title':'another-window','expression':'false'}},{'role':'roles/iam.serviceAccountUser','members':['user:operator@example.test'],'condition':{'expression':'false'}}]}));sys.exit(0)
    if args[:2] == ['auth', 'print-access-token']:
        if fault == 'token_empty': sys.exit(0)
        sys.stdout.write('TEST_TOKEN_NEVER_PERSIST'); sys.exit(0)
    if args[:2] == ['projects', 'get-ancestors']:
        print('project\nfolder' if fault == 'no_org' else 'project\nfolder\norganization')
    elif args[:2] == ['projects', 'describe']:
        print('1' if 'projectNumber' in text else 'pmax')
    elif args[:3] == ['billing', 'projects', 'describe']:
        print('True')
    elif args[:2] == ['auth', 'list']:
        print('operator@example.test')
    elif args[:3] == ['resource-manager', 'org-policies', 'describe']:
        print('{"spec":{"rules":[{"enforce":true,"allowAll":true}]}}')
    elif args[:3] == ['run', 'jobs', 'describe']:
        if 'metadata.labels' in text: print('{"metadata":{"labels":{"app":"pmax","env":"verify","keep":"yes"}}}')
        else: sys.exit(1)
    elif args[:3] == ['storage', 'buckets', 'describe']:
        print('{"labels":{"app":"pmax","env":"verify","keep":"yes"}}')
    elif args[:2] == ['projects', 'get-iam-policy']:
        print('{"version":3,"etag":"test","bindings":[{"role":"roles/viewer","members":["user:editor@example.test"]}]}')
    elif args[:3] == ['storage', 'objects', 'describe']:
        print('test-generation')
    elif 'policies list' in text: print('')
    elif '--format=none' not in args:
        print('user:editor@example.test serviceAccount:agent-a@example.test')
    sys.exit(0)
if command == 'bq':
    leaf = next((x for x in args if x in ('head', 'ls', 'show', 'query', 'update', 'mk')), '')
    if not impersonated:
        if leaf == 'ls':
            # Real bq prints an empty body, not '[]', for a dataset with no tables.
            if fault == 'positive_no_table' or (fault == 'no_table' and args[-1] != os.environ['PROJECT'] + ':' + os.environ['DATASET_REPORTING']): pass
            elif fault == 'malformed_resolution': print('[null]')
            elif fault == 'view_first' and args[-1].endswith(':' + os.environ['DATASET_REPORTING']):
                page = [{'type': kind, 'tableReference': {'tableId': name}} for kind, name in [('VIEW', 'a_view'), ('SNAPSHOT', 'b_snapshot'), ('TABLE', 'probe_table')]]
                limit = int(next(a.split('=')[1] for a in args if a.startswith('--max_results=')))
                print(json.dumps(page[:limit]))
            else: print(json.dumps([{'type': 'VIEW' if fault == 'positive_view' else 'TABLE', 'tableReference': {'tableId': os.environ.get('U18_TABLE', 'probe_table')}}]))
        elif leaf == 'show': print('{"labels":{"app":"pmax","env":"verify","keep":"yes"}}')
        sys.exit(0)
    if 'pmax-looker@' not in impersonated: sys.exit(93)
    reporting = ':' + os.environ['DATASET_REPORTING'] + '.' in text or '.' + os.environ['DATASET_REPORTING'] + '.' in text
    if leaf == 'head' and reporting:
        if fault in ('positive', 'missing_job'):
            print('Access Denied: Permission bigquery.tables.getData denied', file=sys.stderr); sys.exit(1)
        if fault == 'positive_empty': print('[]')
        else: print('[{"value":"ROW_DATA_MUST_NOT_BE_RECORDED"}]')
    elif leaf == 'ls':
        values = [os.environ['DATASET_REPORTING']] + (['pmax_raw'] if fault == 'list' else [])
        print(json.dumps([{'datasetReference': {'datasetId': x}} for x in values]))
    elif leaf == 'query' and os.environ.get('U18_NO_JOB'):
        print('Access Denied: Permission bigquery.jobs.create denied', file=sys.stderr); sys.exit(1)
    elif (leaf == 'head' and fault == 'read') or (leaf == 'show' and fault == 'describe') or (leaf == 'query' and fault == ('create' if 'CREATE TABLE' in text else 'select')):
        print('[]')
    elif fault == 'not_found':
        print('Not found: table', file=sys.stderr); sys.exit(1)
    elif fault == 'mint_failure':
        print('ERROR: (gcloud.auth.print-access-token) PERMISSION_DENIED: Permission iam.serviceAccounts.getAccessToken denied', file=sys.stderr); sys.exit(1)
    elif fault == 'quota':
        print('BigQuery error in query operation: Quota exceeded: Your project exceeded quota.', file=sys.stderr); sys.exit(1)
    elif fault == 'network':
        print('connection reset', file=sys.stderr); sys.exit(1)
    else:
        permission = 'bigquery.datasets.get' if leaf == 'show' else ('bigquery.tables.create' if 'CREATE TABLE' in text else 'bigquery.tables.getData')
        detail = 'Access Denied: Dataset test-pmax-project:pmax_raw: Permission ' + permission + ' denied on dataset test-pmax-project:pmax_raw (or it may not exist).'
        if fault == 'resource_403': detail = 'BigQuery error in head operation: 403 GET https://bigquery.googleapis.com/bigquery/v2/projects/test-pmax-404/datasets/pmax_raw/tables/probe_table/data: ' + detail
        elif fault == 'job_403': detail = "BigQuery error in query operation: Error processing job 'test-pmax-project:bqjob_r404': " + detail
        elif fault == 'structured': detail = json.dumps({'error': {'code': 403, 'errors': [{'reason':'accessDenied','message': detail}]}})
        print(detail, file=sys.stderr); sys.exit(1)
'''
        for command in ('gcloud', 'bq', 'sleep', 'uv', 'docker'):
            file = self.bin / command
            file.write_text(f'#!{sys.executable}\n' + shim)
            file.chmod(0o755)

    def run_phase(self, phase, extra='', **values):
        env = {**self.env, **values}
        functions = '\n'.join(re.findall(r'^\w+\(\) \{\n.*?^\}',
            (root / 'deploy/deploy.sh').read_text(), re.M | re.S))
        script = 'set -euo pipefail\n' + functions + '\nsource "$1"\n' + extra
        return subprocess.run(['bash', '-c', script, 'bash', str(root / 'deploy/phases' / phase)],
                              env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    def calls(self):
        return [json.loads(x) for x in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_organization_required(self):
        result = self.run_phase('00-preflight.sh', PLAN='1', U18_FAULT='no_org', CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('parent organization', result.stdout)

    def test_preflight_exports_configured_reporting_and_all_datasets(self):
        result = self.run_phase('00-preflight.sh', PLAN='1', CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertEqual(result.returncode, 0, result.stdout)
        state = Path(self.env['PHASE_STATE']).read_text()
        self.assertIn('DATASET_REPORTING=pmax_reporting', state)
        self.assertIn('DATASET_REPORTING_VERIFY=pmax_reporting_verify', state)
        self.assertIn('DATASETS_CSV=', state)
        self.assertIn('PMAX_ENV=verify', state)
        self.assertNotIn('editor@example.test', state)

    def test_all_resource_label_flags(self):
        config = yaml.safe_load(self.config.read_text())
        original_datasets = dict(config['datasets'])
        config['datasets']['reporting'] = '9reporting'
        self.config.write_text(yaml.safe_dump(config, sort_keys=False))
        self.assertEqual(yaml.safe_load(self.config.read_text())['datasets'],
                         {**original_datasets, 'reporting': '9reporting'})
        # Reject duplicate root mappings even if the YAML loader accepts them.
        keys = [key.value for key, _ in yaml.compose(self.config.read_text()).value]
        self.assertEqual(keys.count('datasets'), 1, 'fixture has duplicate datasets mappings')
        preflight = self.run_phase('00-preflight.sh', PLAN='1', CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertEqual(preflight.returncode, 0, preflight.stdout)
        state = dict(line.split('=', 1) for line in Path(self.env['PHASE_STATE']).read_text().splitlines())
        # Mimic a future config export; phase 20 must not maintain its own list.
        state['DATASETS_CSV'] += ',future_dataset'
        result = self.run_phase('20-datasets-buckets.sh', PLAN='1', **state)
        self.assertEqual(result.returncode, 0, result.stdout)
        for dataset in state['DATASETS_CSV'].split(','):
            lines = [x for x in result.stdout.splitlines() if '--set_label=app:pmax' in x and f':{dataset} ' in x]
            self.assertEqual(len(lines), 1, f'missing dataset label update: {dataset}')
            self.assertIn('--set_label=env:verify', lines[0])
        # Report, config and the Cloud Build staging bucket carry the pack labels.
        self.assertEqual(result.stdout.count('--update-labels=app=pmax\\,env=verify'), 3)
        self.assertIn('gs://' + state.get('PROJECT', self.env.get('PROJECT', '')) + '_cloudbuild', result.stdout)

    def test_looker_iam_bindings(self):
        result = self.run_phase('40-iam.sh', PLAN='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        lines = result.stdout.splitlines()
        self.assertTrue(any('pmax-looker@' in x and 'roles/bigquery.jobUser' in x for x in lines))
        self.assertTrue(any('--dataset=pmax_reporting ' in x and 'roles/bigquery.dataViewer' in x and '--member=serviceAccount:pmax-looker@' in x for x in lines))
        for principal in ('agent-a', 'agent-b'):
            matches = [x for x in lines if f'{principal}@example.test' in x]
            self.assertEqual(len(matches), 1)
            self.assertIn('pmax-looker@', matches[0])
            self.assertIn('roles/iam.serviceAccountTokenCreator', matches[0])
            self.assertIn('--format=none', matches[0])
        for principal in ('editor', 'second'):
            matches = [x for x in lines if f'user:{principal}@example.test' in x]
            self.assertEqual(len(matches), 1)
            self.assertIn('pmax-looker@', matches[0])
            self.assertIn('roles/iam.serviceAccountUser', matches[0])
        conditional = [x for x in lines if 'pmax-looker@' in x and 'operator@example.test' in x]
        self.assertEqual(len(conditional), 1)
        self.assertIn('request.time', conditional[0])
        self.assertIn('2099-01-01', conditional[0])

    def test_live_iam_does_not_record_editor_or_agent_addresses(self):
        result = self.run_phase('40-iam.sh', ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        texts = [result.stdout] + [x.read_text() for x in Path(self.env['WORK_DIR']).rglob('*') if x.is_file()]
        for text in texts:
            self.assertNotIn('@example.test', text)
        digest = hashlib.sha256(self.config.read_bytes()).hexdigest()[:12]
        record = self.work / 'deployments' / self.env['PROJECT'] / f'looker-iam-{digest}.json'
        self.assertTrue(record.exists(), 'missing durable IAM record')
        self.assertNotIn('@', record.read_text())
        if os.environ.get('U18_PRIVACY_EVIDENCE'):
            destination = Path(os.environ['U18_PRIVACY_EVIDENCE'])
            destination.mkdir(parents=True, exist_ok=True)
            (destination / 'iam-stdout.txt').write_text(result.stdout)
            for path in Path(self.env['WORK_DIR']).rglob('*'):
                if path.is_file():
                    shutil.copy2(path, destination / path.name)
            shutil.copy2(record, destination / record.name)
        self.assertIn('Existing conditional operator bindings: 2', result.stdout)
        self.assertEqual(json.loads(record.read_text())['editors_count'], 2)

    def test_live_iam_requires_future_expiry(self):
        for expiry in ('', '2020-01-01T00:00:00Z', 'not-a-timestamp'):
            with self.subTest(expiry=expiry):
                result = self.run_phase('40-iam.sh', PMAX_LOOKER_PROBE_EXPIRES_AT=expiry)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('PMAX_LOOKER_PROBE_EXPIRES_AT', result.stdout)
                self.assertFalse(any('add-iam-policy-binding' in x['args'] for x in self.calls()))

    def test_reader_mapping_and_redacted_helper_output(self):
        spec = importlib.util.spec_from_file_location('u18_grant', root / 'deploy/lib/grant_dataset_access.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        member = 'serviceAccount:reader@example.test'
        entry = module.entry_for(member, 'roles/bigquery.dataViewer')
        self.assertEqual((entry.role, entry.entity_type, entry.entity_id), ('READER', 'userByEmail', member.split(':')[1]))
        output = io.StringIO()
        with patch('google.cloud.bigquery.Client'), patch.object(module, 'grant', return_value='granted'), contextlib.redirect_stdout(output):
            self.assertEqual(module.main(['--project=test', '--dataset=reporting', '--member='+member, '--role=roles/bigquery.dataViewer']), 0)
        self.assertNotIn('reader@example.test', output.getvalue())

    def test_probe_precheck_only_reads_and_keeps_no_evidence(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes precheck\n', U18_NO_JOB='1')
        self.assertEqual(result.returncode, 0, result.stdout)
        calls = [x for x in self.calls() if x['impersonated']]
        self.assertEqual(len(calls), 1)
        self.assertIn('head', calls[0]['args'])
        operator_calls = [x for x in self.calls() if x['command'] == 'bq' and not x['impersonated']]
        self.assertEqual(len(operator_calls), 1)
        self.assertEqual(operator_calls[0]['args'][-3:], ['ls', '--max_results=20', 'test-pmax-project:pmax_reporting'])
        self.assertEqual(calls[0]['args'][-3:], ['head', '--max_rows=1', 'test-pmax-project:pmax_reporting.probe_table'])
        self.assertNotIn('ROW_DATA_MUST_NOT_BE_RECORDED', result.stdout)
        self.assertFalse((self.work / 'deployments').exists())

    def probe_record(self, directory=None):
        records = list((directory or self.work).glob('deployments/*/looker-probes-*.json'))
        self.assertEqual(len(records), 1)
        return json.loads(records[0].read_text())

    def assert_probe_targets(self, route, table, *, directory=None, reporting=None,
                             dataset_csv=None, positive_table='probe_table'):
        calls = [x for x in self.calls() if x['command'] == 'bq' and x['impersonated']]
        reporting = reporting or self.env['DATASET_REPORTING']
        denied = set((dataset_csv or self.env['DATASETS_CSV']).split(',')) - {reporting}
        project = self.env['PROJECT']
        positive = f'{project}:{reporting}.{positive_table}'
        self.assertIn(['head', '--max_rows=1', positive],
                      [call['args'][-3:] for call in calls if 'head' in call['args']])
        for leaf in ('head', 'show', 'query'):
            selected = [x['args'] for x in calls if leaf in x['args']]
            if route == 'synthetic' and leaf in ('head', 'query'):
                # An empty dataset has no provable table read: no head beyond the
                # positive control and no SELECT probe may be issued.
                selected = [a for a in selected if a[-1] != positive and not a[-1].startswith('CREATE')]
                self.assertEqual(selected, [])
                continue
            if leaf == 'head':
                selected = [a for a in selected if a[-1] != positive]
                self.assertEqual({a[-1] for a in selected}, {f"{project}:{d}.{table}" for d in denied})
                targets = {a[-1].split('.')[0] for a in selected}
                self.assertTrue(all(a[-1].endswith('.' + table) for a in selected))
            elif leaf == 'show':
                targets = {a[-1] for a in selected}
            else:
                selected = [a for a in selected if a[-1].startswith('SELECT')]
                self.assertEqual({a[-1] for a in selected},
                                 {f'SELECT 1 FROM `{project}.{d}.{table}` LIMIT 1' for d in denied})
                targets = {re.search(r'FROM `([^`]+)`', a[-1])[1].rsplit('.', 1)[0].replace('.', ':', 1) for a in selected}
                self.assertTrue(all(f'.{table}` LIMIT 1' in a[-1] for a in selected))
                self.assertTrue(all('--maximum_bytes_billed=1048576' in a for a in selected))
            self.assertEqual(targets, {f'{project}:{d}' for d in denied})
            self.assertEqual(len(selected), len(denied))
        record = self.probe_record(directory)
        self.assertEqual(record['status'], 'PASSED')
        for permission in ('tables.getData', 'datasets.get', 'tables.getData.query'):
            probes = [x for x in record['probes'] if x['permission'] == permission and x['dataset'] in denied]
            self.assertEqual({x['dataset'] for x in probes}, denied)
            for probe in probes:
                self.assertEqual((probe['route'], probe['table'], probe['resolved_by']), (route, table, 'operator'))
                expected = 'NOT_PROVABLE_EMPTY_DATASET' if route == 'synthetic' and permission != 'datasets.get' else 'PERMISSION_DENIED'
                self.assertEqual(probe['outcome'], expected)
        for dataset in denied:
            slot = record['audit_log'][dataset]
            self.assertIsNone(slot['datasets.get'])
            self.assertEqual(slot['tables.getData'], 'NOT_PROVABLE_EMPTY_DATASET' if route == 'synthetic' else None)
        self.assertNotIn('ROW_DATA_MUST_NOT_BE_RECORDED', json.dumps(record))
        return record

    def test_full_probe_permission_shapes(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        record = self.assert_probe_targets('existing', 'probe_table')
        self.assertEqual(record['probes'][0]['row_count'], 1)
        self.assertNotIn('@example.test', json.dumps(record))
        if os.environ.get('U18_PRIVACY_EVIDENCE'):
            destination = Path(os.environ['U18_PRIVACY_EVIDENCE'])
            destination.mkdir(parents=True, exist_ok=True)
            (destination / 'probe-record.json').write_text(json.dumps(record, indent=2))
            (destination / 'probe-stdout.txt').write_text(result.stdout)
        base_args = ['--project_id=test-pmax-project', '--location=EU', '--format=json',
                     '--use_gcloud_config=true', '--use_gcloud_config_cache=false', '--quiet',
                     '--request_reason=' + record['request_reason']]
        calls = [x['args'] for x in self.calls() if x['command'] == 'bq' and x['impersonated']]
        for leaf in [
            ['head', '--max_rows=1', 'test-pmax-project:pmax_reporting.probe_table'],
            ['ls', '--datasets', '--all', '--max_results=2', 'test-pmax-project:'],
            ['head', '--max_rows=1', 'test-pmax-project:pmax_raw.probe_table'],
            ['show', '--dataset', 'test-pmax-project:pmax_raw'],
            ['query', '--use_legacy_sql=false', '--maximum_bytes_billed=1048576', 'SELECT 1 FROM `test-pmax-project.pmax_raw.probe_table` LIMIT 1'],
        ]:
            self.assertIn(base_args + leaf, calls)

    def test_empty_denied_dataset_uses_synthetic_name(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', U18_FAULT='no_table', ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assert_probe_targets('synthetic', 'pmax_probe_missing')

    def test_every_probe_inversion_and_error_fails(self):
        faults = {
            'positive': 'positive control failed within the five-minute retry window',
            'positive_no_table': 'no table in reporting dataset pmax_reporting',
            'positive_view': 'no table in reporting dataset pmax_reporting',
            'malformed_resolution': 'operator table resolution returned invalid data',
            'list': 'must return exactly the reporting dataset',
            'read': 'tables.getData pmax_raw: expected denial, received success',
            'describe': 'datasets.get pmax_raw: expected denial, received success',
            'create': 'tables.create pmax_reporting: expected denial, received success',
            'select': 'tables.getData.query pmax_raw: expected denial, received success',
            'not_found': 'expected resource access denial, received another error',
            'network': 'expected resource access denial, received another error',
            'mint_failure': 'expected resource access denial, received another error',
            'quota': 'expected resource access denial, received another error',
        }
        for fault, reason in faults.items():
            with self.subTest(fault=fault):
                directory = self.work / fault
                result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', U18_FAULT=fault, ROOT=str(directory))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(reason, result.stdout)
                record = self.probe_record(directory)
                self.assertEqual(record['status'], 'FAILED')
                self.assertIn(reason, record['failure'])

    def test_resource_denial_error_forms(self):
        for fault in ('real_transcript', 'resource_403', 'job_403', 'structured'):
            with self.subTest(fault=fault):
                result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', U18_FAULT=fault, ROOT=str(self.work / fault))
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(self.probe_record(self.work / fault)['status'], 'PASSED')

    def test_valid_dataset_and_table_names(self):
        for dataset, table in (('9reporting', 'probe_table'), ('pmax_reporting', '9table'),
                               ('pmax_reporting', 'table-with-dashes'), ('pmax_reporting', 'étudiant 01')):
            with self.subTest(table=table):
                self.log.unlink(missing_ok=True)
                directory = self.work / table
                result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', ROOT=str(directory),
                                        DATASET_REPORTING=dataset, DATASETS_CSV=dataset + ',pmax_raw', U18_TABLE=table)
                self.assertEqual(result.returncode, 0, result.stdout)
                record = self.assert_probe_targets('existing', table, directory=directory,
                                                   reporting=dataset, dataset_csv=dataset + ',pmax_raw',
                                                   positive_table=table)
                self.assertEqual(record['probes'][0]['table'], table)
                literal = json.dumps(f'projects/{self.env["PROJECT"]}/datasets/{dataset}/tables/{table}', ensure_ascii=False)
                self.assertIn('protoPayload.resourceName=' + literal, record['audit_log_corroboration']['filter'])

    def test_positive_control_skips_non_table_objects(self):
        for mode in ('precheck', 'full'):
            with self.subTest(mode=mode):
                self.log.unlink(missing_ok=True)
                directory = self.work / mode
                result = self.run_phase('looker-probes.sh', '\nlooker_probes ' + mode + '\n',
                                        ROOT=str(directory), U18_FAULT='view_first')
                self.assertEqual(result.returncode, 0, result.stdout)
                resolutions = [x['args'][-3:] for x in self.calls() if x['command'] == 'bq'
                               and not x['impersonated'] and x['args'][-1] == 'test-pmax-project:pmax_reporting']
                self.assertEqual(resolutions, [['ls', '--max_results=20', 'test-pmax-project:pmax_reporting']])
                heads = [x['args'][-3:] for x in self.calls() if x['command'] == 'bq' and 'head' in x['args']]
                self.assertIn(['head', '--max_rows=1', 'test-pmax-project:pmax_reporting.probe_table'], heads)
                if mode == 'full':
                    self.assert_probe_targets('existing', 'probe_table', directory=directory)
                else:
                    self.assertFalse((directory / 'deployments').exists())

    def test_positive_empty_table_proves_permission(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', ROOT=str(self.work), U18_FAULT='positive_empty')
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.probe_record()['probes'][0]['row_count'], 0)

    def test_operator_resolution_clears_inherited_impersonation(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', ROOT=str(self.work),
                                CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT='inherited@example.test')
        self.assertEqual(result.returncode, 0, result.stdout)
        calls = [x for x in self.calls() if x['command'] == 'bq' and 'ls' in x['args'] and '--datasets' not in x['args']]
        self.assertEqual(len(calls), len(self.env['DATASETS_CSV'].split(',')))
        self.assertTrue(all(x['impersonated'] == '' for x in calls))
        self.assertEqual(self.probe_record()['probes'][0]['resolved_by'], 'operator')

    def test_audit_corroboration_has_executable_filter_and_slots(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        record = self.probe_record()
        audit = record['audit_log_corroboration']
        self.assertEqual(audit['argv'][:3], ['gcloud', 'logging', 'read'])
        self.assertEqual(audit['argv'][3], audit['filter'])
        self.assertEqual(audit['argv'], ['gcloud', 'logging', 'read', audit['filter'],
                                         '--project=test-pmax-project', '--freshness=30d',
                                         '--order=asc', '--format=json', '--quiet'])
        self.assertEqual(shlex.split(audit['command']), ['env', 'CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=', *audit['argv']])
        for value in (record['principal'], record['request_reason'], record['started_at'], record['finished_at']):
            self.assertIn(value, audit['filter'])
        denied = set(self.env['DATASETS_CSV'].split(',')) - {'pmax_reporting'}
        self.assertEqual(set(record['audit_log']), denied)
        for dataset, slot in record['audit_log'].items():
            self.assertIn('projects/test-pmax-project/datasets/' + dataset, audit['filter'])
            self.assertEqual(slot, {'tables.getData': None, 'datasets.get': None})
        self.assertFalse(any(x['args'][:2] == ['logging', 'read'] for x in self.calls()))

    def test_no_job_right_still_reaches_query_probes(self):
        result = self.run_phase('looker-probes.sh', '\nlooker_probes full\n', U18_NO_JOB='1', ROOT=str(self.work))
        self.assertNotEqual(result.returncode, 0)
        calls = [x for x in self.calls() if x['impersonated']]
        self.assertTrue(any('head' in x['args'] for x in calls))
        self.assertTrue(any('ls' in x['args'] for x in calls))
        self.assertTrue(any('show' in x['args'] for x in calls))
        self.assertIn('bigquery.jobs.create', result.stdout)

    def test_handover_documents_conditional_revocation(self):
        doc = (root / 'deploy/iam.md').read_text()
        self.assertIn('gcloud iam service-accounts remove-iam-policy-binding', doc)
        self.assertIn('--condition="expression=request.time < timestamp(', doc)
        self.assertIn('looker-iam-<config-sha256-prefix>.json', doc)
        self.assertIn('audit_log', doc)

    def test_existing_conditional_bindings_counted(self):
        result = self.run_phase('40-iam.sh', ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('Existing conditional operator bindings: 2', result.stdout)
        calls = self.calls()
        get = next(i for i, call in enumerate(calls) if call['args'][:3] == ['iam', 'service-accounts', 'get-iam-policy'])
        add = next(i for i, call in enumerate(calls) if any(arg.startswith('--condition=expression') for arg in call['args']))
        self.assertLess(get, add)

    def test_iam_failure_is_redacted(self):
        result = self.run_phase('40-iam.sh', U18_FAULT='iam_failure', ROOT=str(self.work))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('IAM action failed', result.stdout)
        self.assertNotIn('@', result.stdout)

    def test_host_independent_docker_presence(self):
        # This executable proves presence only; preflight must never run it.
        self.assertTrue((self.bin / 'docker').is_file())
        result = subprocess.run([str(self.bin / 'docker'), 'version'], env=self.env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 97)
        result = self.run_phase('00-preflight.sh', PLAN='1', CONFIG_LOCAL=str(self.work / 'validated.yaml'))
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_runtime_token_mint_never_opens_token_file(self):
        # A directory at the old path makes the previous redirection fail.
        (Path(self.env['WORK_DIR']) / 'runtime-token-probe').mkdir()
        # Exercise the real token block independently from the later twin
        # and anchor prerequisites, covered in test_deploy_rehearsal.sh.
        source = (root / 'deploy/phases/88-rehearsal.sh').read_text()
        start = source.index('if [[ "$PLAN" -eq 1 ]]; then\n  print_command gcloud auth')
        end = source.index('run_cmd gcloud run jobs execute', start)
        token_phase = self.work / 'runtime-token-phase.sh'
        token_phase.write_text(source[start:end])
        result = self.run_phase(str(token_phase), ROOT=str(self.work))
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertNotIn('TEST_TOKEN_NEVER_PERSIST', result.stdout)
        for path in self.work.glob('deployments/**/*.json'):
            self.assertNotIn('TEST_TOKEN_NEVER_PERSIST', path.read_text())
        result = self.run_phase(str(token_phase), ROOT=str(self.work), U18_FAULT='token_empty')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('runtime impersonation returned no token', result.stdout)

    def test_cli_shims_reject_bad_flags(self):
        for command, args, flag in (
            ('bq', ['head', '--max_rowz=1', 'test-pmax-project:pmax_raw.probe_table'], '--max_rowz=1'),
            ('gcloud', ['projects', 'get-ancestors', 'test-pmax-project', '--formatt=value(type)'], '--formatt=value(type)'),
        ):
            with self.subTest(command=command):
                result = subprocess.run([str(self.bin / command), *args], env=self.env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn(flag, result.stderr)


unittest.main(argv=['u18', *([os.environ['U18_CASE']] if os.environ.get('U18_CASE') else [])], verbosity=2)
PY_U18
[[ "${U18_TESTS_ONLY:-0}" != 1 ]] || exit 0

PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
FAKE_BQ_LOG="$TMP/bq.log" \
FAKE_DOCKER_LOG="$TMP/docker.log" \
FAKE_CONFIG="$TMP/config.yaml" \
  "$DEPLOY" \
    --project test-pmax-project \
    --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" \
    --plan >"$TMP/plan.out"

# These commands must be emitted by both phases, with their invocation reason.
assert_contains "$TMP/plan.out" 'PLAN  env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=pmax-looker@'
for mode in precheck full; do
  grep -E "^PLAN  env CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=pmax-looker@.*--request_reason=pmax-looker-$mode-.*head --max_rows=1" \
    "$TMP/plan.out" >/dev/null || fail "missing impersonated $mode positive control in plan"
done
assert_contains "$TMP/plan.out" "Config source: local first-deploy file"
assert_contains "$TMP/plan.out" "gcloud storage cp"
assert_contains "$TMP/plan.out" "gs://test-config-bucket/test.yaml"
assert_contains "$TMP/plan.out" \
  "signed review fields: run_id,image_digest,report_uri,parity_run_id,reviewer,reviewed_at,decision"
assert_contains "$TMP/plan.out" \
  "WHERE run_id = @run_id AND event = 'EXITED'"

expected_specs=(
  "00-preflight|agent-safe"
  "10-apis|human-run"
  "20-datasets-buckets|agent-safe"
  "25-dry-run|agent-safe"
  "30-secret|agent-safe"
  "40-iam|human-run"
  "45-wif|human-run"
  "50-build-deploy|agent-safe"
  "55-invoker|human-run"
  "60-scheduler|agent-safe"
  "65-config|agent-safe"
  "68-migration|human-run"
  "70-first-run|agent-safe"
  "75-lease-drill|agent-safe"
  "80-parity|agent-safe"
  "85-review|human-run"
  "88-rehearsal|agent-safe"
  "89-retention|human-run"
  "90-alert|agent-safe"
  "95-resume|human-run"
)
previous=0
for spec in "${expected_specs[@]}"; do
  phase="${spec%%|*}"
  owner="${spec##*|}"
  line="$(grep -nF "PHASE $phase [$owner]" "$TMP/plan.out" | cut -d: -f1)"
  [[ -n "$line" ]] || fail "plan omitted exact owner mapping $spec"
  (( line > previous )) || fail "phase order is wrong at $phase"
  previous="$line"
done
if grep -Fq 'plan-output-canary-must-never-appear' "$TMP/plan.out"; then
  fail "plan leaked credential contents"
fi

while IFS= read -r call; do
  case "$call" in
    projects\ describe\ *|projects\ get-ancestors\ *|billing\ projects\ describe\ *|auth\ list\ *|resource-manager\ org-policies\ describe\ *|run\ jobs\ describe\ *)
      ;;
    *) fail "--plan executed a mutating or unexpected gcloud leaf: $call" ;;
  esac
done <"$TMP/gcloud.log"
[[ ! -s "$TMP/bq.log" ]] || fail "--plan executed bq instead of printing it"
[[ ! -s "$TMP/docker.log" ]] || fail "--plan executed docker instead of printing it"

: >"$TMP/fresh-missing.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/fresh-missing.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --plan \
    >"$TMP/fresh-missing.out" 2>&1; then
  fail "fresh deploy without --config-file was accepted"
fi
assert_contains "$TMP/fresh-missing.out" "first deploy requires --config-file PATH"
if grep -Fq 'storage cp' "$TMP/fresh-missing.log"; then
  fail "fresh deploy tried to use a GCS object instead of requiring --config-file"
fi

: >"$TMP/upgrade.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade.log" \
FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-plan.out"
assert_contains "$TMP/upgrade-plan.out" "Config source: GCS upgrade object"
# Live-found 2026-08-28: the upgrade branch must read the existing image through the v1 job shape;
# a wrong path returns an empty string and the branch refuses a digest-pinned job.
assert_contains "$TMP/upgrade-plan.out" '--format=value\(spec.template.spec.template.spec.containers\[0\].image\)'
assert_contains "$TMP/upgrade.log" "storage cp gs://test-config-bucket/test.yaml"

same_digest_root="$TMP/same-digest-root"
same_digest_record="$same_digest_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$same_digest_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$same_digest_record"
: >"$TMP/same-digest-gcloud.log"
: >"$TMP/same-digest-bq.log"
PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/same-digest-gcloud.log" \
FAKE_BQ_LOG="$TMP/same-digest-bq.log" \
FAKE_BQ_RESPONSE='[{"row_count":"12","observed_days":"3","latest_observed_day":"2026-09-18"}]' \
FAKE_JOB_EXISTS=1 \
FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PLAN=0 UPGRADE=1 ROOT="$same_digest_root" \
PROJECT=test-pmax-project REGION=europe-west1 \
DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
CONFIG_LOCAL="$TMP/config.yaml" \
PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PMAX_MIGRATION_REVIEWED=1 \
run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/same-digest.out"
[[ "$(<"$same_digest_record")" == \
  'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' ]] || \
  fail "same-digest upgrade rewrote the rollback pointer"
assert_contains "$TMP/same-digest.out" "same-digest redeploy: keeping recorded rollback image"

transition_root="$TMP/transition-root"
transition_record="$transition_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$transition_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$transition_record"
: >"$TMP/transition-gcloud.log"
: >"$TMP/transition-bq.log"
PATH="$TMP/bin:$PATH" \
FAKE_GCLOUD_LOG="$TMP/transition-gcloud.log" \
FAKE_BQ_LOG="$TMP/transition-bq.log" \
FAKE_BQ_RESPONSE='[{"row_count":"12","observed_days":"3","latest_observed_day":"2026-09-18"}]' \
FAKE_JOB_EXISTS=1 \
FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
PLAN=0 UPGRADE=1 ROOT="$transition_root" \
PROJECT=test-pmax-project REGION=europe-west1 \
DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
CONFIG_LOCAL="$TMP/config.yaml" \
PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:next \
PMAX_MIGRATION_REVIEWED=1 \
run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/transition.out"
[[ "$(<"$transition_record")" == \
  'europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current' ]] || \
  fail "real-transition upgrade did not rewrite the rollback pointer"
assert_contains "$TMP/transition.out" \
  "recorded previous immutable image europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current"

malformed_root="$TMP/malformed-ref-root"
malformed_record="$malformed_root/deployments/test-pmax-project/previous-image.txt"
mkdir -p "$(dirname "$malformed_record")"
printf '%s\n' 'europe-west1-docker.pkg.dev/test/repo/image@sha256:rollback' \
  >"$malformed_record"
cp "$malformed_record" "$TMP/malformed-ref.before"
: >"$TMP/malformed-ref-gcloud.log"
: >"$TMP/malformed-ref-bq.log"
if PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/malformed-ref-gcloud.log" \
  FAKE_BQ_LOG="$TMP/malformed-ref-bq.log" \
  FAKE_JOB_EXISTS=1 \
  FAKE_JOB_IMAGE=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:current \
  PLAN=0 UPGRADE=1 ROOT="$malformed_root" \
  PROJECT=test-pmax-project REGION=europe-west1 \
  DATASET_RAW=pmax_raw DATASET_MARTS=pmax_marts \
  CONFIG_LOCAL="$TMP/config.yaml" \
  PMAX_IMAGE_REF=europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack:mutable \
  PMAX_MIGRATION_REVIEWED=1 \
  run_phase "$PHASES/25-dry-run.sh" deploy noop capture none \
  >"$TMP/malformed-ref.out" 2>&1; then
  fail "phase 25 accepted a malformed PMAX_IMAGE_REF"
fi
assert_contains "$TMP/malformed-ref.out" \
  "PMAX_IMAGE_REF must be a digest-pinned image in europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack"
cmp "$TMP/malformed-ref.before" "$malformed_record" || \
  fail "malformed PMAX_IMAGE_REF changed the rollback pointer"

: >"$TMP/upgrade-missing.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade-missing.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=0 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-missing.out" 2>&1; then
  fail "upgrade without its recorded GCS config object was accepted"
fi
assert_contains "$TMP/upgrade-missing.out" "upgrade requires the existing GCS config object"
assert_contains "$TMP/upgrade-missing.out" "404 config object not found"

: >"$TMP/upgrade-local.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/upgrade-local.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/upgrade-local.out" 2>&1; then
  fail "upgrade accepted --config-file instead of the recorded object"
fi
assert_contains "$TMP/upgrade-local.out" "--config-file is only valid for a first deploy"
if grep -Fq 'storage cp' "$TMP/upgrade-local.log"; then
  fail "upgrade touched config storage after refusing --config-file"
fi

mkdir -p "$TMP/config-record-root"
: >"$TMP/config-phase.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/config-phase.log" FAKE_CONFIG="$TMP/config.yaml" \
FAKE_UPLOADED_CONFIG="$TMP/uploaded-config.yaml" \
PLAN=0 UPGRADE=0 ROOT="$TMP/config-record-root" WORK_DIR="$TMP" \
PROJECT=test-pmax-project REGION=europe-west1 \
CONFIG_LOCAL="$TMP/config.yaml" CONFIG_URI=gs://test-config-bucket/test.yaml \
IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:abc \
SECRET_NAME=pmax-google-ads SECRET_VERSION=7 OAUTH_STATUS=production \
OPERATOR_IDENTITY=operator@example.test \
run_phase "$PHASES/65-config.sh" plain execute capture none
cmp "$TMP/config.yaml" "$TMP/uploaded-config.yaml" || \
  fail "phase 65 did not upload the validated first-deploy config"
assert_contains "$TMP/config-phase.log" "storage cp $TMP/config.yaml gs://test-config-bucket/test.yaml"

: >"$TMP/config-upgrade-phase.log"
PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/config-upgrade-phase.log" FAKE_CONFIG="$TMP/config.yaml" \
PLAN=0 UPGRADE=1 ROOT="$TMP/config-record-root" WORK_DIR="$TMP" \
PROJECT=test-pmax-project REGION=europe-west1 \
CONFIG_LOCAL="$TMP/config.yaml" CONFIG_URI=gs://test-config-bucket/test.yaml \
IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:abc \
SECRET_NAME=pmax-google-ads SECRET_VERSION=7 OAUTH_STATUS=production \
OPERATOR_IDENTITY=operator@example.test \
run_phase "$PHASES/65-config.sh" plain execute capture none
if grep -Fq 'storage cp' "$TMP/config-upgrade-phase.log"; then
  fail "upgrade phase 65 overwrote its recorded config truth"
fi

if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_PROJECT_LABEL=wrong \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-label.out" 2>&1; then
  fail "project without app=pmax was accepted"
fi
assert_contains "$TMP/bad-label.out" "app=pmax"

sed 's/project: test-pmax-project/project: another-project/' \
  "$TMP/config.yaml" >"$TMP/config-mismatch.yaml"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/gcloud.log" \
  FAKE_CONFIG="$TMP/config-mismatch.yaml" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config-mismatch.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-config.out" 2>&1; then
  fail "config deployment.project mismatch was accepted"
fi
assert_contains "$TMP/bad-config.out" "deployment.project"

if PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_KEY_CREATION_POLICY='{"spec":{"rules":[{"enforce":false}]}}' \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/bad-policy.out" 2>&1; then
  fail "preflight accepted an effective policy that allows service-account keys"
fi
assert_contains "$TMP/bad-policy.out" "disable service-account key creation"

if PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_ALLOWED_DOMAINS_POLICY='{"spec":{"rules":[{"denyAll":true}]}}' \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/config.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/deny-all.out" 2>&1; then
  fail "preflight accepted an effective policy that denies every IAM member"
fi
assert_contains "$TMP/deny-all.out" "denies every IAM member"

sed 's/mcc: "2345678901"/mcc: "9999999999"/' \
  "$TMP/config.yaml" >"$TMP/top-mcc.yaml"
PATH="$TMP/bin:$PATH" FAKE_CONFIG="$TMP/top-mcc.yaml" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --config-file "$TMP/top-mcc.yaml" \
    --credential-file "$TMP/credential.yaml" --plan >"$TMP/top-mcc.out"
assert_contains "$TMP/top-mcc.out" \
  "deployed config accounts list is the only extraction bound"

# Legacy credential cases execute the real continuation helper through Bash startup.
cp "$TMP/ladder-functions.sh" "$TMP/preflight-helpers.sh"
run_preflight_case() {
  local config="$1"
  local failed_account="$2"
  local label="$3"
  : >"$TMP/$label-gcloud.log"
  : >"$TMP/$label-uv.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/$label-gcloud.log" \
  FAKE_UV_LOG="$TMP/$label-uv.log" \
  FAKE_CONFIG="$config" \
  FAKE_PROBE_FAIL_ACCOUNT="$failed_account" \
  BASH_ENV="$TMP/preflight-helpers.sh" \
  PLAN=0 UPGRADE=0 ROOT="$ROOT" \
  PROJECT=test-pmax-project REGION=europe-west1 \
  CONFIG_FILE="$config" CONFIG_LOCAL="$TMP/$label-config.yaml" \
  CONFIG_URI=gs://test-config-bucket/test.yaml \
  PHASE_STATE="$TMP/$label-state.env" \
  CREDENTIAL_FILE="$TMP/credential.yaml" \
  run_phase "$PHASES/00-preflight.sh" deploy none capture none
}

run_preflight_case "$TMP/top-mcc.yaml" "" top-mcc-probe \
  >"$TMP/top-mcc-probe.out"
assert_contains "$TMP/top-mcc-probe.out" \
  "deployed config accounts list is the only extraction bound"
assert_contains "$TMP/top-mcc-probe-uv.log" \
  "run pmax-pack probe --credential-file $TMP/credential.yaml --account 1234567890"

if run_preflight_case "$TMP/config.yaml" 1234567890 single-account \
  >"$TMP/single-account.out" 2>&1; then
  fail "preflight accepted a credential that could not resolve its allowlisted account"
fi
assert_contains "$TMP/single-account.out" "1234567890"

sed 's/accounts: \["1234567890"\]/accounts: ["1234567890", "3456789012"]/' \
  "$TMP/config.yaml" >"$TMP/config-extra-account.yaml"
if run_preflight_case "$TMP/config-extra-account.yaml" 3456789012 extra-account \
  >"$TMP/extra-account.out" 2>&1; then
  fail "preflight accepted an extra configured account that the credential could not resolve"
fi
assert_contains "$TMP/extra-account.out" "3456789012"
assert_contains "$TMP/extra-account-uv.log" \
  "run pmax-pack probe --credential-file $TMP/credential.yaml --account 3456789012"

# Exercise secret branching as a sourced phase with a fake gcloud.
run_secret_case() {
  local exists="$1"
  local probe_ok="$2"
  local upgrade="${3:-0}"
  local pinned_file="${4:-$TMP/config.yaml}"
  local candidate_file="${5:-$TMP/credential.yaml}"
  : >"$TMP/secret.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/secret.log" \
  FAKE_CONFIG="$TMP/config.yaml" \
  FAKE_SECRET_EXISTS="$exists" \
  FAKE_PINNED_SECRET_FILE="$pinned_file" \
  FAKE_ADDED_SECRET_FILE="$candidate_file" \
  PREFLIGHT_PROBE_OK="$probe_ok" \
  PROJECT=test-pmax-project \
  CREDENTIAL_FILE="$candidate_file" \
  SECRET_NAME=pmax-google-ads \
  PMAX_OAUTH_PUBLISHING_STATUS=production \
  ACCOUNTS_CSV=1234567890 \
  UPGRADE="$upgrade" \
  PLAN=0 \
  WORK_DIR="$TMP" \
  ROOT="$TMP/product" \
  PHASE_STATE="$TMP/phase-state" \
  run_phase "$PHASES/30-secret.sh" plain execute capture none
}

run_secret_case 1 1
assert_contains "$TMP/secret.log" "secrets describe pmax-google-ads"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"
if grep -Fq 'secrets create' "$TMP/secret.log"; then
  fail "existing secret incorrectly used create"
fi

run_secret_case 0 1
assert_contains "$TMP/secret.log" "secrets create pmax-google-ads"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"

if run_secret_case 1 0 >"$TMP/probe-fail.out" 2>&1; then
  fail "secret phase accepted failed candidate probe"
fi
if [[ -s "$TMP/secret.log" ]]; then
  fail "secret phase touched Secret Manager after failed probe"
fi

if run_secret_case 1 1 0 "$TMP/config.yaml" "$TMP/missing-credential.yaml" \
  >"$TMP/missing-credential.out" 2>&1; then
  fail "secret phase accepted a missing operator credential file"
fi
assert_contains "$TMP/missing-credential.out" "operator credential file is missing"
if [[ -s "$TMP/secret.log" ]]; then
  fail "secret phase touched Secret Manager without the operator credential file"
fi

mkdir -p "$TMP/product/deployments/test-pmax-project"
cat >"$TMP/product/deployments/test-pmax-project/deployment.yaml" <<'YAML'
sm_resource: pmax-google-ads
sm_version: 6
YAML
run_secret_case 1 1 1 "$TMP/credential.yaml"
if grep -Fq 'secrets versions add' "$TMP/secret.log"; then
  fail "unchanged candidate fingerprint added a redundant secret version"
fi
assert_contains "$TMP/secret.log" "secrets versions access 6"

run_secret_case 1 1 1 "$TMP/config.yaml"
assert_contains "$TMP/secret.log" "secrets versions add pmax-google-ads"

for guarded in \
  "$PHASES/20-datasets-buckets.sh" \
  "$PHASES/30-secret.sh" \
  "$PHASES/40-iam.sh" \
  "$PHASES/45-wif.sh" \
  "$PHASES/60-scheduler.sh"; do
  assert_contains "$guarded" "describe"
done

uv run python - "$PHASES/40-iam.sh" "$PHASES/45-wif.sh" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

for path_text in sys.argv[1:]:
    path = Path(path_text)
    logical_lines: list[str] = []
    current = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        current += raw.strip() + " "
        if not raw.rstrip().endswith("\\"):
            logical_lines.append(current)
            current = ""
    assert not [line for line in logical_lines if "bq add-iam-policy-binding" in line], path
    grants = [line for line in logical_lines if "grant_dataset_access.py" in line]
    assert grants, path
    assert all("--dataset=" in line and "--member=" in line and "--role=" in line for line in grants), grants
PY

assert_contains "$PHASES/55-invoker.sh" "--role=roles/run.invoker"
if grep -Fq -- '--condition=None' "$PHASES/55-invoker.sh"; then
  fail "job IAM leaf still carries unsupported --condition=None"
fi
assert_contains "$PHASES/45-wif.sh" "--member=\"\$WIF_MEMBER\" --role=roles/bigquery.jobUser"
if grep -Eq "serviceAccount:\\\$CI_SA|workloadIdentityUser" "$PHASES/45-wif.sh"; then
  fail "WIF phase still bridges through a service account"
fi
assert_contains "$PHASES/50-build-deploy.sh" "imagetools inspect"
assert_contains "$PHASES/50-build-deploy.sh" "published image manifest lacks linux/amd64"
assert_contains "$PHASES/70-first-run.sh" "PMAX_FIRST_RUN_MAX_EXECUTIONS"
assert_contains "$PHASES/70-first-run.sh" "pending_after"
assert_contains "$PHASES/70-first-run.sh" "credential_fingerprint does not match the pinned secret"
assert_contains "$PHASES/80-parity.sh" "PMAX_PARITY_LOCAL_CONFIRMED"
if grep -Fq 'gcloud run jobs execute' "$PHASES/80-parity.sh"; then
  fail "phase 80 still executes parity as the Cloud Run runtime identity"
fi
assert_contains "$PHASES/65-config.sh" "sm_resource: \$SECRET_NAME"
assert_contains "$PHASES/65-config.sh" "sm_version: \$SECRET_VERSION"
assert_private_doc "$ROOT/RUNBOOK.md" 'previous-image.txt'
assert_private_doc "$ROOT/RUNBOOK.md" "No \`docker build\`, \`gcloud builds submit\`"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "| pMax Performance Pack | Secret Manager pinned version"
assert_private_doc "$ROOT/RUNBOOK.md" "The pack rotates with the shared MCC token"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "including the pack's Secret Manager"
assert_private_doc "$ROOT/RUNBOOK.md" \
  "then verifies its SUCCESS ledger row and its image digest externally"
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  'For phase 80, run the `LOCAL` command printed by the live phase'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  '`sha256:PLAN_DIGEST` placeholder'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  '`image_digest mismatch` refusal'
assert_contains "$ROOT/deploy/review-template.yaml" 'run_id: "<phase-70-run-id>"'
assert_contains "$ROOT/deploy/review-template.yaml" 'decision: GO'
assert_private_doc "$ROOT/RUNBOOK.md" 'signed-review-validation-<digest>.json'
assert_private_doc "$ROOT/RUNBOOK.md" 'reviewed_at predates phase 70'
assert_private_doc "$ROOT/RUNBOOK.md" 'single-invocation pause'
assert_private_doc "$ROOT/RUNBOOK.md" 'written operator authorization'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'agents and automation may never create or supply it'
assert_private_doc "$ROOT/RUNBOOK.md" \
  "operator's sign-off for the exact evidence"
assert_private_doc "$ROOT/RUNBOOK.md" 'PMAX_IMAGE_REF=<printed-recorded-image-digest>'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'First invocation: leave PMAX_PARITY_LOCAL_CONFIRMED and PMAX_SIGNED_REVIEW unset'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'Re-run with both PMAX_PARITY_LOCAL_CONFIRMED and PMAX_SIGNED_REVIEW set'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'exactly one SUCCESS and one SKIPPED across the two current drill executions, regardless of which won'
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_MIGRATION_REVIEWED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_PARITY_LOCAL_CONFIRMED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_SIGNED_REVIEW=\"\$SIGNED_REVIEW\" \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_ALERT_CONFIRMED=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" "PMAX_SKIPPED_ALERT_SILENT=1 \\"
assert_private_doc "$ROOT/RUNBOOK.md" \
  'same no-build ladder as a phase-85 retry'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'phase may never appear in the confirmation list'
assert_contains "$ROOT/docs/operations.md" "shared manager-account (MCC) token"
assert_contains "$ROOT/docs/operations.md" "sets \`PMAX_IMAGE_REF\` to the prior digest"
assert_contains "$ROOT/deploy/iam.md" "shared manager-account (MCC) token"
assert_contains "$PHASES/75-lease-drill.sh" \
  'Exactly one SUCCESS and one SKIPPED across the two drill executions, regardless of which won.'
if [[ ! -f "$ROOT/INDEX.md" ]]; then
  if private_tree; then
    fail "INDEX.md is missing from the private tree"
  fi
  echo "SKIP: $ROOT/INDEX.md absent (private file not part of this export)"
elif ! grep -Eq '^\| RUNBOOK\.md \|.*\| 2026-09-27 \|$' "$ROOT/INDEX.md"; then
  fail "INDEX.md does not date the RUNBOOK row to 2026-09-27"
fi

# Private-tree rollback pinning: the generic anchor phrases above hold in
# every tree; where the deployment evidence exists, the documents must also
# carry the exact recorded values (a wrong digest must fail, not pass).
if private_tree; then
  # Locate the anchor by glob so no deployment identifier ships in this file.
  ANCHOR_CANDIDATES=("$ROOT"/deployments/*/rollback-anchor.txt)
  [[ ${#ANCHOR_CANDIDATES[@]} -eq 1 && -f "${ANCHOR_CANDIDATES[0]}" ]] || \
    fail "expected exactly one deployments/*/rollback-anchor.txt in the private tree"
  ANCHOR_FILE="${ANCHOR_CANDIDATES[0]}"
  assert_contains "$ANCHOR_FILE" "anchor_digest="
  ANCHOR_DIGEST="$(sed -n 's/^anchor_digest=//p' "$ANCHOR_FILE")"
  FPFIX_COMMIT="$(sed -n 's/^fingerprint_fix_commit=//p' "$ANCHOR_FILE")"
  [[ -n "$ANCHOR_DIGEST" && -n "$FPFIX_COMMIT" ]] || \
    fail "rollback-anchor.txt is missing anchor_digest or fingerprint_fix_commit"
  assert_contains "$ROOT/STATUS.md" "$ANCHOR_DIGEST"
  assert_contains "$ROOT/RUNBOOK.md" "$ANCHOR_DIGEST"
  assert_contains "$ROOT/RUNBOOK.md" "$FPFIX_COMMIT"
fi

# Self-test of the private-doc guard's three states (mutants in a scratch
# ROOT so the discriminator itself is exercised, not assumed).
guard_selftest() {
  local scratch missing_out
  scratch="$(mktemp -d "${TMPDIR:-/tmp}/pmax-guard-selftest.XXXXXX")"
  printf 'needle here\n' >"$scratch/present.md"
  ( ROOT="$scratch" assert_private_doc "$scratch/present.md" "needle here" ) \
    || fail "guard self-test: present file with needle must pass"
  ( ROOT="$scratch" assert_private_doc "$scratch/present.md" "absent needle" ) \
    && fail "guard self-test: present file without needle must fail"
  missing_out="$( ROOT="$scratch" assert_private_doc "$scratch/missing.md" "x" )" \
    || fail "guard self-test: missing file in export mode must SKIP"
  [[ "$missing_out" == SKIP:* ]] || fail "guard self-test: export mode must print SKIP"
  mkdir -p "$scratch/deployments"
  ( ROOT="$scratch" assert_private_doc "$scratch/missing.md" "x" ) \
    && fail "guard self-test: missing file in private tree must fail"
  rm -rf "$scratch"
  echo "PASS: private-doc guard self-test (present, export-missing, private-missing)"
}
guard_selftest
assert_private_doc "$ROOT/STATUS.md" "pointer is self-referential"
assert_private_doc "$ROOT/STATUS.md" "avoid the RUNBOOK rollback recipe"
assert_private_doc "$ROOT/STATUS.md" \
  'is the standing rollback anchor'
assert_private_doc "$ROOT/RUNBOOK.md" \
  'is the standing rollback anchor'
# shellcheck disable=SC2016
assert_private_doc "$ROOT/RUNBOOK.md" \
  'images built before fingerprint fix commit'
assert_private_doc "$ROOT/plans/reviews/2026-08-29-fix-round-r2-status.md" \
  '## Operator rulings (2026-08-30)'

run_build_case() {
  local phase_path="$1"
  local inspection="$2"
  local requested_image_ref="${3:-}"
  : >"$TMP/build-gcloud.log"
  : >"$TMP/build-docker.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_GCLOUD_LOG="$TMP/build-gcloud.log" \
  FAKE_DOCKER_LOG="$TMP/build-docker.log" \
  FAKE_IMAGE_INSPECTION="$inspection" \
  PLAN=0 UPGRADE=0 ROOT="$(mktemp -d "$TMP/build-root.XXXXXX")" PROJECT=test-pmax-project REGION=europe-west1 \
  CONFIG_URI=gs://test-config-bucket/test.yaml \
  REPORT_BUCKET=test-report-bucket \
  RUNTIME_SA=pmax-runtime@test-pmax-project.iam.gserviceaccount.com \
  BUILD_SA=pmax-build@test-pmax-project.iam.gserviceaccount.com \
  SECRET_NAME=pmax-google-ads SECRET_VERSION=7 \
  PMAX_IMAGE_REF="$requested_image_ref" \
  run_phase "$phase_path" plain execute capture none
}

amd64_manifest='{"manifest":{"platform":{"os":"linux","architecture":"amd64"}}}'
arm64_manifest='{"manifest":{"platform":{"os":"linux","architecture":"arm64"}}}'
run_build_case "$PHASES/50-build-deploy.sh" "$amd64_manifest"
assert_contains "$TMP/build-docker.log" "buildx imagetools inspect"
# The six-hour Job timeout is paired with the normal and rebuild lease's seven-hour budget.
assert_contains "$TMP/build-gcloud.log" "--task-timeout=6h"
assert_contains "$TMP/build-gcloud.log" \
  "PMAX_REPORT_BUCKET=test-report-bucket"
if run_build_case "$PHASES/50-build-deploy.sh" "$arm64_manifest" \
  >"$TMP/arm64.out" 2>&1; then
  fail "build phase accepted a manifest lacking linux/amd64"
fi
assert_contains "$TMP/arm64.out" "lacks linux/amd64"

reused_image="europe-west1-docker.pkg.dev/test-pmax-project/pmax-pack/pmax-pack@sha256:0123456789abcdef"
run_build_case "$PHASES/50-build-deploy.sh" "$amd64_manifest" "$reused_image"
[[ ! -s "$TMP/build-docker.log" ]] || \
  fail "PMAX_IMAGE_REF path invoked docker instead of reusing the digest"
assert_contains "$TMP/build-gcloud.log" \
  "artifacts docker images describe $reused_image --project=test-pmax-project"
assert_contains "$TMP/build-gcloud.log" \
  "run jobs deploy pmax-pack-daily --project=test-pmax-project --region=europe-west1 --image=$reused_image --service-account=pmax-runtime@test-pmax-project.iam.gserviceaccount.com --labels=app=pmax,env=prod"

sed '/capture_cmd IMAGE_INSPECTION docker buildx imagetools inspect/,+1d' \
  "$PHASES/50-build-deploy.sh" >"$TMP/50-no-inspect.sh"
if run_build_case "$TMP/50-no-inspect.sh" "$amd64_manifest" \
  >"$TMP/no-inspect.out" 2>&1; then
  fail "deleted-inspect mutant survived"
fi

run_parity_case() {
  local response="$1"
  local label="$2"
  local record_root="${3:-$TMP/$label-root}"
  local run_record_preserved="${4:-0}"
  mkdir -p "$record_root"
  : >"$TMP/$label-bq.log"
  PATH="$TMP/bin:$PATH" \
  FAKE_BQ_LOG="$TMP/$label-bq.log" \
  FAKE_BQ_RESPONSE="$response" \
  PLAN=0 ROOT="$record_root" PROJECT=test-pmax-project DATASET_OPS=pmax_ops \
  ACCOUNTS_CSV=1234567890 RUN_DAY=2026-08-27 \
  CONFIG_LOCAL="$TMP/config.yaml" \
  CONFIG_URI=gs://test-config-bucket/deployment.yaml \
  CREDENTIAL_FILE="$TMP/credential.yaml" \
  IMAGE_REF=europe-west1-docker.pkg.dev/test/repo/image@sha256:current \
  RUN_RECORD_PRESERVED="$run_record_preserved" \
  OPERATOR_IDENTITY=operator@example.test PMAX_PARITY_LOCAL_CONFIRMED=1 \
  run_phase "$PHASES/80-parity.sh" deploy none none none
}

test_parity_image_digest_gate() {
  local matching_response failed_response mismatch_response
  local missing_detail_response missing_digest_response
  local malformed_detail_response invalid_detail_response
  local wrong_query_hash_response wrong_reference_commit_response
  local wrong_api_version_response
  matching_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  failed_response='[{"run_id":"parity-1234567890-2026-08-27","status":"FAILED","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  mismatch_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:previous\"}"}]'
  missing_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS"}]'
  missing_digest_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"passed\":true}"}]'
  malformed_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{not json"}]'
  invalid_detail_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"null"}]'
  wrong_query_hash_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"wrong-query-hash\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v25\"}"}]'
  wrong_reference_commit_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"wrong-reference-commit\",\"api_version\":\"v25\"}"}]'
  wrong_api_version_response='[{"run_id":"parity-1234567890-2026-08-27","status":"SUCCESS","detail":"{\"image_digest\":\"europe-west1-docker.pkg.dev/test/repo/image@sha256:current\",\"query_hash\":\"239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528\",\"reference_commit\":\"9790e8a585b6e6f76851efed3e9b42ad87d8d97c\",\"api_version\":\"v24\"}"}]'

  run_parity_case "$matching_response" parity-match >"$TMP/parity-match.out"
  assert_contains "$TMP/parity-match.out" \
    "PMAX_CONFIG=gs://test-config-bucket/deployment.yaml"
  if grep -Fq -- "$TMP/config.yaml" "$TMP/parity-match.out"; then
    fail "phase 80 LOCAL line references the temporary WORK_DIR config"
  fi
  local parity_record="$TMP/parity-match-root/deployments/test-pmax-project/parity-evidence-sha256-current.json"
  [[ -f "$parity_record" ]] || \
    fail "phase 80 omitted digest-keyed parity evidence"
  assert_contains "$parity_record" '"parity_run_id": "parity-1234567890-2026-08-27"'
  assert_contains "$parity_record" '"date": "2026-08-27"'
  assert_contains "$parity_record" \
    '"image_digest": "europe-west1-docker.pkg.dev/test/repo/image@sha256:current"'
  assert_contains "$parity_record" \
    '"query_hash": "239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528"'
  assert_contains "$parity_record" \
    '"reference_commit": "9790e8a585b6e6f76851efed3e9b42ad87d8d97c"'
  assert_contains "$parity_record" '"api_version": "v25"'
  assert_contains "$parity_record" '"resource_labels":'
  assert_contains "$parity_record" '"pmax_reporting":'
  assert_contains "$parity_record" '"test-config-bucket":'
  assert_contains "$parity_record" '"pmax-pack-daily":'
  assert_contains "$parity_record" '"other_label_count": 1'
  if grep -Eq '"keep"|"yes"' "$parity_record"; then
    fail "parity evidence retained a foreign label"
  fi

  uv run python - "$parity_record" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
record = json.loads(path.read_text())
record["parity_run_id"] = "parity-preserved"
for key in ("api_version", "query_hash", "reference_commit"):
    record.pop(key)
path.write_text(json.dumps(record, indent=2) + "\n")
PY
  run_parity_case "$matching_response" parity-rerun \
    "$TMP/parity-match-root" 1 >"$TMP/parity-rerun.out"
  assert_contains "$parity_record" '"parity_run_id": "parity-preserved"'
  assert_contains "$parity_record" \
    '"query_hash": "239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528"'
  assert_contains "$parity_record" \
    '"reference_commit": "9790e8a585b6e6f76851efed3e9b42ad87d8d97c"'
  assert_contains "$parity_record" '"api_version": "v25"'
  assert_contains "$parity_record" '"resource_labels":'
  assert_contains "$parity_record" '"pmax_reporting":'
  assert_contains "$parity_record" '"test-config-bucket":'
  assert_contains "$parity_record" '"pmax-pack-daily":'
  assert_contains "$parity_record" '"other_label_count": 1'
  if grep -Eq '"keep"|"yes"' "$parity_record"; then
    fail "parity evidence retained a foreign label"
  fi

  if FAKE_LABEL_ENV=verify run_parity_case "$matching_response" parity-wrong-env \
    >"$TMP/parity-wrong-env.out" 2>&1; then
    fail "phase 80 accepted mismatched resource env"
  fi
  assert_contains "$TMP/parity-wrong-env.out" "resource labels do not match"

  if run_parity_case "$failed_response" parity-failed \
    >"$TMP/parity-failed.out" 2>&1; then
    fail "phase 80 accepted a FAILED parity ledger row"
  fi
  assert_contains "$TMP/parity-failed.out" "lacks a SUCCESS ledger row"

  if run_parity_case "$mismatch_response" parity-mismatch \
    >"$TMP/parity-mismatch.out" 2>&1; then
    fail "phase 80 accepted parity evidence from a different image digest"
  fi
  assert_contains "$TMP/parity-mismatch.out" "image_digest mismatch"

  if run_parity_case "$missing_detail_response" parity-missing-detail \
    >"$TMP/parity-missing-detail.out" 2>&1; then
    fail "phase 80 accepted parity evidence without detail"
  fi
  assert_contains "$TMP/parity-missing-detail.out" "missing or invalid detail"

  if run_parity_case "$missing_digest_response" parity-missing-digest \
    >"$TMP/parity-missing-digest.out" 2>&1; then
    fail "phase 80 accepted parity detail without image_digest"
  fi
  assert_contains "$TMP/parity-missing-digest.out" "image_digest mismatch"

  if run_parity_case "$malformed_detail_response" parity-malformed-detail \
    >"$TMP/parity-malformed-detail.out" 2>&1; then
    fail "phase 80 accepted malformed parity detail"
  fi
  assert_contains "$TMP/parity-malformed-detail.out" "missing or invalid detail"

  if run_parity_case "$invalid_detail_response" parity-invalid-detail \
    >"$TMP/parity-invalid-detail.out" 2>&1; then
    fail "phase 80 accepted parity evidence with invalid detail"
  fi
  assert_contains "$TMP/parity-invalid-detail.out" "missing or invalid detail"

  if run_parity_case "$wrong_query_hash_response" parity-wrong-query-hash \
    >"$TMP/parity-wrong-query-hash.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong query_hash"
  fi
  assert_contains "$TMP/parity-wrong-query-hash.out" \
    "operator-run local parity query_hash mismatch: expected 239226e4370d2e3f1f9d59473bca03ebdc3b52e0313200a3c0eeb719d9040528, got wrong-query-hash"

  if run_parity_case "$wrong_reference_commit_response" parity-wrong-reference-commit \
    >"$TMP/parity-wrong-reference-commit.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong reference_commit"
  fi
  assert_contains "$TMP/parity-wrong-reference-commit.out" \
    "operator-run local parity reference_commit mismatch: expected 9790e8a585b6e6f76851efed3e9b42ad87d8d97c, got wrong-reference-commit"

  if run_parity_case "$wrong_api_version_response" parity-wrong-api-version \
    >"$TMP/parity-wrong-api-version.out" 2>&1; then
    fail "phase 80 accepted parity detail with the wrong api_version"
  fi
  assert_contains "$TMP/parity-wrong-api-version.out" \
    "operator-run local parity api_version mismatch: expected v25, got v24"
  assert_contains "$TMP/parity-match-bq.log" "SELECT run_id, status, detail FROM"
}

test_parity_image_digest_gate

uv run python - "$PR_WORKFLOW" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

pr_path = Path(sys.argv[1])
workflows = pr_path.parent
# v2.0.1: no trusted parity workflow ships; public CI is fixture-only and never
# federates to GCP. Real-data parity runs from the operator's deploy ladder.
assert not (workflows / "trusted.yml").exists(), "trusted.yml must not ship"
assert not (workflows / "trusted.yaml").exists(), "trusted.yaml must not ship"
workflow_paths = sorted(
    p for ext in ("*.yml", "*.yaml") for p in workflows.glob(ext)
)
assert workflow_paths, "no workflow files found"
for path in workflow_paths:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # A non-UTF-8 sidecar (macOS AppleDouble) is not a workflow.
        continue
    assert "google-github-actions/auth" not in text, path
    assert "workload_identity_provider" not in text, path
    assert "id-token" not in text, path
    assert "pull_request_target" not in text, path
    assert "google-ads" not in text.lower(), path
    uses = [line for line in text.splitlines() if "uses:" in line]
    assert uses, path
    for line in uses:
        assert re.search(r"@[0-9a-f]{40}\s+#\s+v", line), line

pr = yaml.safe_load(pr_path.read_text(encoding="utf-8"))
assert pr["permissions"] == {"contents": "read"}
triggers = pr.get("on", pr.get(True))
assert "main" in triggers["push"]["branches"]
assert "pull_request" in triggers
PY

(cd "$ROOT" && uv run pytest -q tests/unit/test_scrub_check.py \
  -k 'oidc_permission or oidc_exception')


# Operator gate and loud failures: exercise the real function bodies in isolation.
DEPLOY_FUNCS="$TMP/deploy-funcs.sh"
sed -n '/^die() {/,/^}/p; /^print_command() {/,/^}/p; /^run_cmd() {/,/^}/p; /^confirm_human_phase() {/,/^}/p' \
  "$ROOT/deploy/deploy.sh" >"$DEPLOY_FUNCS"
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES='' bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 10-apis" \
  </dev/null >"$TMP/gate.out" 2>&1; then
  fail "a human-owned phase ran without operator confirmation in a non-interactive run"
fi
assert_contains "$TMP/gate.out" "needs the operator: list it in PMAX_CONFIRMED_PHASES"
PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="40-iam,10-apis" bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 10-apis" \
  </dev/null >/dev/null 2>&1 || fail "an explicitly confirmed human-owned phase was refused"
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="10-apis" bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 40-iam" \
  </dev/null >/dev/null 2>&1; then
  fail "a phase outside PMAX_CONFIRMED_PHASES was allowed"
fi
if PLAN=0 ASSUME_YES=0 PMAX_CONFIRMED_PHASES="10-apis,85-review" \
  bash -c "source '$DEPLOY_FUNCS'; confirm_human_phase 85-review" \
  </dev/null >"$TMP/review-confirmed.out" 2>&1; then
  fail "PMAX_CONFIRMED_PHASES was allowed to pre-confirm phase 85"
fi
assert_contains "$TMP/review-confirmed.out" \
  "PMAX_CONFIRMED_PHASES must not list 85-review"

# F1 (round-3 confirmation): a pre-confirmed 85-review must be refused before ANY phase runs, not at 85's turn.
: >"$TMP/early-review.log"
if PATH="$TMP/bin:$PATH" FAKE_GCLOUD_LOG="$TMP/early-review.log" \
  FAKE_CONFIG="$TMP/config.yaml" FAKE_JOB_EXISTS=1 FAKE_CONFIG_OBJECT_EXISTS=1 \
  PMAX_CONFIRMED_PHASES="10-apis,85-review" \
  "$DEPLOY" --project test-pmax-project --region europe-west1 \
    --config-uri gs://test-config-bucket/test.yaml \
    --credential-file "$TMP/credential.yaml" --upgrade --plan \
    >"$TMP/early-review.out" 2>&1; then
  fail "a ladder with 85-review pre-confirmed was allowed to start"
fi
assert_contains "$TMP/early-review.out" "PMAX_CONFIRMED_PHASES must not list 85-review"
[[ ! -s "$TMP/early-review.log" ]] || fail "85-review pre-confirmation was refused only after phases ran"
if PLAN=0 bash -c "source '$DEPLOY_FUNCS'; run_cmd false" >"$TMP/loud.out" 2>&1; then
  fail "a failing phase command did not stop the ladder"
fi
assert_contains "$TMP/loud.out" "phase command failed"

assert_contains "$ROOT/deploy/phases/25-dry-run.sh" "first deploy: cost dry-run skipped"
# shellcheck disable=SC2016
assert_contains "$ROOT/deploy/phases/68-migration.sh" '--target-dataset "$DATASET_MARTS" --dry-run'
assert_contains "$ROOT/deploy/phases/88-rehearsal.sh" 'DATASET_MARTS,--dry-run'
assert_contains "$ROOT/deploy/phases/88-rehearsal.sh" 'DATASET_VERIFY"'
# shellcheck disable=SC2016
if grep -Fq -- '--target-dataset "$DATASET_VERIFY" --dry-run' "$ROOT/deploy/phases/25-dry-run.sh"; then
  fail "phase 25 still dry-runs into the empty verification dataset"
fi

if grep -Fq -- '--dimensions="user=' "$ROOT/deploy/phases/45-wif.sh"; then
  fail "phase 45 still requests a per-principal quota dimension (unsupported live)"
fi
assert_contains "$ROOT/deploy/phases/45-wif.sh" '--unit="1/d/{project}/{user}"'

assert_contains "$ROOT/deploy/phases/45-wif.sh" 'PMAX_CI_DAILY_QUERY_QUOTA_MIB'
assert_contains "$ROOT/deploy/phases/45-wif.sh" 'resource.label."service"="bigquery.googleapis.com"'
assert_contains "$ROOT/deploy/phases/45-wif.sh" 'metric.label."quota_metric"'
if grep -Fq 'metric.label."service"' "$ROOT/deploy/phases/45-wif.sh"; then
  fail "quota alert still filters on a metric label that does not exist on consumer_quota"
fi

assert_contains "$ROOT/deploy/phases/45-wif.sh" 'ALIGN_COUNT_TRUE'
if awk '/quota\/exceeded/,/ALIGN_/' "$ROOT/deploy/phases/45-wif.sh" | grep -q ALIGN_DELTA; then
  fail "quota-exceeded alert still uses ALIGN_DELTA on a BOOL gauge"
fi

if [[ "$(grep -n 'configure-docker' "$ROOT/deploy/phases/50-build-deploy.sh" | head -1 | cut -d: -f1)" -gt "$(grep -n 'docker buildx build' "$ROOT/deploy/phases/50-build-deploy.sh" | head -1 | cut -d: -f1)" ]]; then
  fail "phase 50 must configure the Artifact Registry credential helper before the push"
fi
# shellcheck disable=SC2016
assert_contains "$ROOT/deploy/phases/50-build-deploy.sh" 'configure-docker "$REGION-docker.pkg.dev"'

for split_suite in lib.sh test_deploy_review.sh test_deploy_first_run.sh; do
  [[ -f "$ROOT/deploy/tests/$split_suite" ]] || \
    fail "deploy harness split is missing $split_suite"
done
assert_contains "$ROOT/Makefile" "deploy-test:"
assert_contains "$ROOT/Makefile" "bash deploy/tests/test_deploy_review.sh"
assert_contains "$ROOT/Makefile" "bash deploy/tests/test_deploy_first_run.sh"

bash "$ROOT/deploy/tests/test_deploy_rehearsal.sh"

echo "PASS: deploy plan output, refusals, migration, retention, phase-25 pointer, phase-80 parity, and CI contracts"
