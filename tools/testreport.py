"""Render gotestsum JUnit as offline HTML and a GitHub job summary (stdlib only).

Counts come from testcase elements, including parent/subtests as emitted by JUnit,
not aggregate suite counters. Reports describe tests, not workflow success.
"""
import argparse
from collections import Counter
from html import escape
import math
import os
import re
from pathlib import Path
import xml.etree.ElementTree as ET

STATUSES = ('passed', 'failed', 'error', 'skipped')


def duration(value):
    number = float(value or '0')
    if not math.isfinite(number) or number < 0:
        raise ValueError('invalid duration')
    return number


def output_details(element):
    details = []
    for child in element:
        if child.tag in ('failure', 'error', 'skipped', 'system-out', 'system-err'):
            text = '\n'.join(filter(None, [child.get('message'), child.get('type'), child.text]))
            details.append((child.tag, text))
    return details


def load_report(path, module):
    report = dict(module=module, state='NO RESULTS', counts=dict.fromkeys(STATUSES, 0), suites=[], issue='')
    try:
        data = Path(path).read_bytes()
        # JUnit needs no DTD. Reject entities before parsing untrusted PR output.
        # Normalize UTF-16/32 NULs as well, rather than allowing an encoded DTD.
        if b'<!DOCTYPE' in data.replace(b'\x00', b'').upper() or b'<!ENTITY' in data.replace(b'\x00', b'').upper():
            raise ValueError('DTD/entities are not supported')
        root = ET.fromstring(data)
        if root.tag not in ('testsuite', 'testsuites'):
            raise ValueError('not a JUnit document')
        suites = list(root.iter('testsuite'))
        if not suites:
            raise ValueError('no test suites')
        declared_problem = False
        for suite in suites:
            declared_problem |= any(int(suite.get(key, '0')) > 0 for key in ('failures', 'errors'))
            cases = []
            for case in suite.findall('testcase'):
                status = next((status for tag, status in [('error', 'error'), ('failure', 'failed'), ('skipped', 'skipped')] if case.find(tag) is not None), 'passed')
                cases.append(dict(name=case.get('name', '(unnamed)'), classname=case.get('classname', ''), status=status, seconds=duration(case.get('time')), details=output_details(case)))
            report['suites'].append(dict(name=suite.get('name', '(unnamed suite)'), seconds=duration(suite.get('time')), cases=cases, details=output_details(suite)))
        counts = Counter(case['status'] for suite in report['suites'] for case in suite['cases'])
        report['counts'].update(counts)
        report['state'] = 'FAIL' if declared_problem or counts['failed'] or counts['error'] else 'PASS' if counts else 'NO TEST CASES'
        if declared_problem and not (counts['failed'] or counts['error']):
            report['issue'] = 'Suite reports a failure/error without a corresponding failed testcase; inspect package output and job logs.'
    except (OSError, ET.ParseError, ValueError) as exc:
        report.update(state='NO RESULTS', suites=[], counts=dict.fromkeys(STATUSES, 0), issue=f'No usable report: {exc}. Tests may not have started or completed.')
    return report


def details_html(details):
    return ''.join(f'<details><summary>{escape(kind)}</summary><pre>{escape(text)}</pre></details>' for kind, text in details)


def provenance():
    names = ('GITHUB_REPOSITORY', 'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_EVENT_NAME', 'JOB_STATUS', 'TEST_STATUS', 'UNIT_STATUS', 'E2E_STATUS')
    return ' · '.join(f'{name}: {os.environ[name]}' for name in names if os.environ.get(name)) or 'Local report'


def render(reports):
    links = []
    repo = os.environ.get('GITHUB_REPOSITORY', '')
    if re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+', repo):
        for key, route, label, pattern in [('GITHUB_SHA', 'commit', 'Source commit', r'[a-fA-F0-9]{40}'), ('GITHUB_RUN_ID', 'actions/runs', 'Workflow run', r'[0-9]+')]:
            value = os.environ.get(key, '')
            if re.fullmatch(pattern, value):
                links.append(f'<a href="https://github.com/{repo}/{route}/{value}">{label}</a>')
    parts = [HEADER, f'<p class="provenance">{escape(provenance())}</p>', '<p>' + ' · '.join(links) + '</p>',
             '<p>Counts describe reported JUnit test cases, not build, vet, generation or workflow success. Missing results are not a pass. Parent tests and subtests count exactly as emitted by JUnit.</p>',
             '<div class="filters"><label>Search package or test <input id="search" type="search" placeholder="Test name or package"></label><label>Status <select id="status"><option value="">All statuses</option>' + ''.join(f'<option value="{status}">{status}</option>' for status in STATUSES) + '</select></label><p id="visible" role="status" aria-live="polite"></p></div>']
    for report in reports:
        counts = report['counts']
        summary = ' · '.join(f'{counts[status]} {status}' for status in STATUSES)
        parts += [f'<section><h2>{escape(report["module"])} — {escape(report["state"])}</h2>', f'<p>{sum(counts.values())} cases · {summary}</p>']
        if report['issue']:
            parts.append(f'<p class="issue">{escape(report["issue"])}</p>')
        for suite in report['suites']:
            parts += [f'<article><h3>{escape(suite["name"])} <small>{suite["seconds"]:g}s</small></h3>', details_html(suite['details'])]
            for case in suite['cases']:
                parts += [f'<div class="case" data-status="{case["status"]}"><div class="case-heading"><span class="badge {case["status"]}">{case["status"]}</span><span><span class="classname">{escape(case["classname"])}</span><strong>{escape(case["name"])}</strong></span><span>{case["seconds"]:g}s</span></div>', details_html(case['details']), '</div>']
            parts.append('</article>')
        parts.append('</section>')
    return '\n'.join(parts) + FOOTER


def markdown(reports):
    lines = ['## Test results', '', escape(provenance()), '', '| Module | Report | Cases | Passed | Failed | Errors | Skipped |', '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
    issues = []
    for report in reports:
        c = report['counts']
        module = escape(report['module']).replace('|', '&#124;').replace('\n', ' ')
        lines.append(f'| {module} | {report["state"]} | {sum(c.values())} | {c["passed"]} | {c["failed"]} | {c["error"]} | {c["skipped"]} |')
        if report['issue']:
            issues.append(f'{module}: {escape(report["issue"])}')
    lines += ['', *issues, '', 'Counts describe reported test cases, not workflow success. Download the test-report artifact for searchable HTML and failure/output details.']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reports', type=Path, default=Path('.reports'))
    parser.add_argument('--modules', nargs='+', choices=['gateway', 'protobuf', 'cli', 'e2e'], default=['gateway', 'protobuf', 'cli', 'e2e'])
    parser.add_argument('--output', type=Path, default=Path('.reports/site/index.html'))
    parser.add_argument('--summary', type=Path, help='append a Markdown GitHub job summary')
    args = parser.parse_args()
    reports = [load_report(args.reports / f'{module}.xml', module) for module in args.modules]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(reports), encoding='utf-8')
    if args.summary:
        with args.summary.open('a', encoding='utf-8') as output:
            output.write(markdown(reports))
    for report in reports:
        print(f'{report["module"]}: {report["state"]} ({sum(report["counts"].values())} cases)')


HEADER = '''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>OraCache test report</title><style>
:root{color-scheme:light dark;font-family:system-ui,sans-serif;line-height:1.5;background:#101820;color:#e5edf5}
*{box-sizing:border-box}body{max-width:1100px;margin:auto;padding:24px}h1{margin-bottom:0}h2{font-size:1.3rem}h3{overflow-wrap:anywhere}small,.classname,.provenance{color:#b9c8d7}small{font-weight:normal}section{margin:28px 0;border-top:2px solid #506579}article{margin:16px 0;padding:16px;background:#1b2935;border-radius:8px}.filters{display:flex;align-items:end;gap:16px;flex-wrap:wrap}label{display:grid;gap:6px}input,select{font:inherit;padding:8px;border:1px solid #9aaebe;border-radius:4px;background:#101820;color:inherit}input{width:min(340px,80vw)}.case{border-top:1px solid #506579;padding:12px 0}.case-heading{display:flex;align-items:start;gap:12px}.case-heading>span:nth-child(2){flex:1;min-width:0;overflow-wrap:anywhere}.classname{display:block;font-size:.85rem}.badge{min-width:65px;font-weight:bold}.passed{color:#8fdfac}.failed,.error,.issue{color:#ffb4ac}.skipped{color:#efda8c}summary{cursor:pointer;padding:6px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#101820;padding:12px;max-height:32rem;overflow:auto}.provenance{overflow-wrap:anywhere;font-size:.85rem}[hidden]{display:none!important}@media(max-width:500px){body{padding:12px}article{padding:10px}.case-heading{gap:8px}.badge{min-width:55px}}
</style></head><body><h1>OraCache test report</h1>
'''

FOOTER = '''<script>
const search = document.getElementById('search');
const status = document.getElementById('status');
const cases = [...document.querySelectorAll('.case')];
function filter() {
  const query = search.value.toLowerCase();
  let visible = 0;
  for (const row of cases) {
    const suite = row.closest('article').querySelector('h3').textContent;
    const name = row.querySelector('.case-heading').textContent;
    row.hidden = !(suite + ' ' + name).toLowerCase().includes(query) || (status.value !== '' && row.dataset.status !== status.value);
    if (!row.hidden) visible++;
  }
  document.getElementById('visible').textContent = `${visible} of ${cases.length} cases shown`;
}
search.addEventListener('input', filter);
status.addEventListener('change', filter);
filter();
</script></body></html>
'''

if __name__ == '__main__':
    main()
