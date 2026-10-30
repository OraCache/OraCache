"""Stdlib regression tests; includes a real gotestsum snapshot and synthetic edge cases."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from xml.sax.saxutils import escape, quoteattr

import testreport


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def report(self, xml):
        path = self.root / 'gateway.xml'
        path.write_text(xml, encoding='utf-8')
        return testreport.load_report(path, 'gateway')

    def test_actual_gotestsum_v1_12_fixture(self):
        path = Path(__file__).parent / 'testdata' / 'gotestsum-cli.xml'
        report = testreport.load_report(path, 'cli')
        self.assertEqual(report['state'], 'PASS')
        self.assertEqual(report['counts']['passed'], 18)
        self.assertIn('TestPositionalValues/literal_spaces', testreport.render([report]))

    def test_cases_not_aggregate_counters_or_subtest_name_inference(self):
        report = self.report('''<testsuites tests="99"><testsuite name="outer" tests="3">
          <testsuite name="package" tests="3" time="1.5">
          <testcase name="TestParent" time="0.5"/>
          <testcase name="TestParent/child" time="0.7"/>
          <testcase name="TestSkipped"><skipped message="not available"/></testcase>
          </testsuite></testsuite></testsuites>''')
        self.assertEqual(report['counts'], dict(passed=2, failed=0, error=0, skipped=1))
        self.assertEqual(report['state'], 'PASS')
        page = testreport.render([report])
        self.assertIn('TestParent/child', page)
        self.assertIn('1.5', page)
        self.assertIn('not available', page)

    def test_failure_error_and_output_are_expandable_and_escaped(self):
        attack = '<script>alert("x")</script><img src=x onerror=alert(1)>'
        report = self.report(f'''<testsuite name={quoteattr(attack)}>
          <testcase name={quoteattr(attack)} classname="pkg" time="0.25">
          <failure message={quoteattr(attack)}>{escape(attack)}</failure>
          <system-out>{escape(attack)}</system-out></testcase>
          <testcase name="error"><error message="boom">stack trace</error></testcase>
          <system-err>suite output</system-err></testsuite>''')
        self.assertEqual(report['counts'], dict(passed=0, failed=1, error=1, skipped=0))
        self.assertEqual(report['state'], 'FAIL')
        page = testreport.render([report])
        self.assertNotIn(attack, page)
        self.assertIn('&lt;script&gt;', page)
        self.assertIn('stack trace', page)
        self.assertIn('suite output', page)
        self.assertIn('<details', page)
        self.assertNotIn('innerHTML', page)
        self.assertIn('textContent', page)
        self.assertIn('id="search"', page)
        self.assertIn('id="status"', page)

    def test_missing_malformed_unrecognized_or_dtd_are_no_results(self):
        missing = testreport.load_report(self.root / 'missing.xml', 'missing')
        self.assertEqual(missing['state'], 'NO RESULTS')
        for xml in ['<testsuites>', '<html/>', '<!DOCTYPE x [<!ENTITY a "x">]><testsuite/>', '<testsuite><testcase time="nan"/></testsuite>']:
            with self.subTest(xml=xml):
                self.assertEqual(self.report(xml)['state'], 'NO RESULTS')

    def test_zero_cases_not_passed(self):
        self.assertEqual(self.report('<testsuites><testsuite tests="0"/></testsuites>')['state'], 'NO TEST CASES')
        self.assertEqual(self.report('<testsuites/>')['state'], 'NO RESULTS')

    def test_package_failure_without_cases_is_not_pass(self):
        report = self.report('<testsuite tests="0" errors="1"><system-err>build failed</system-err></testsuite>')
        self.assertEqual(report['state'], 'FAIL')
        self.assertIn('build failed', testreport.render([report]))

    def test_provenance_links_only_for_valid_github_identifiers(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'GITHUB_REPOSITORY': 'OraCache/repo', 'GITHUB_SHA': 'a' * 40, 'GITHUB_RUN_ID': '123'}):
            page = testreport.render([])
        self.assertIn('https://github.com/OraCache/repo/actions/runs/123', page)
        self.assertIn('https://github.com/OraCache/repo/commit/' + 'a' * 40, page)
        with patch.dict(os.environ, {'GITHUB_REPOSITORY': '\"><script>x</script>', 'GITHUB_SHA': 'javascript:alert(1)'}):
            self.assertNotIn('href=', testreport.render([]))

    def test_cli_writes_html_and_summary_even_with_missing_module(self):
        self.report('<testsuite name="pkg"><testcase name="ok"/></testsuite>')
        output = self.root / 'site' / 'index.html'
        summary = self.root / 'summary.md'
        env = dict(os.environ, GITHUB_SHA='<unsafe>', JOB_STATUS='failure', TEST_STATUS='skipped')
        result = subprocess.run([sys.executable, str(Path(testreport.__file__)), '--reports', str(self.root), '--modules', 'gateway', 'e2e', '--output', str(output), '--summary', str(summary)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('NO RESULTS', output.read_text())
        self.assertIn('&lt;unsafe&gt;', output.read_text())
        self.assertIn('failure', summary.read_text())
        self.assertIn('skipped', summary.read_text())
        self.assertIn('gateway', summary.read_text())
        self.assertIn('NO RESULTS', summary.read_text())


if __name__ == '__main__':
    unittest.main()
