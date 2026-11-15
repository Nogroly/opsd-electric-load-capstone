"""Report must reject failed, stale or altered forecast evidence."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'jobs'))
import render_forecast


class ReportGuard(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_out = render_forecast.OUT
        render_forecast.OUT = self.out = Path(self.temp.name)
        content = {'metrics.csv':'model,mae_mw\nlast_hour,1\n', 'predictions.csv':'target_utc,actual_mw\n2019-10-01 00:00:00,10\n'}
        hashes = {}
        for name,text in content.items():
            (self.out/name).write_text(text)
            hashes[name] = hashlib.sha256((self.out/name).read_bytes()).hexdigest()
        self.summary = dict(status='PASSED',run_id='new-run',files=hashes)
        self.proof = dict(status='PASSED',forecast_run_id='new-run',files=hashes)
        self.write()

    def write(self):
        (self.out/'summary.json').write_text(json.dumps(self.summary))
        (self.out/'verification.json').write_text(json.dumps(self.proof))

    def tearDown(self):
        render_forecast.OUT = self.old_out
        self.temp.cleanup()

    def test_matching_evidence_can_be_read(self):
        self.assertEqual(len(render_forecast.verified_inputs()[2]),1)

    def test_failed_run_cannot_display_success(self):
        self.summary['status']='FAILED'
        self.write()
        with self.assertRaises(ValueError):
            render_forecast.verified_inputs()

    def test_old_verification_cannot_validate_new_run(self):
        self.proof['forecast_run_id']='old-run'
        self.write()
        with self.assertRaises(ValueError):
            render_forecast.verified_inputs()

    def test_changed_csv_is_rejected(self):
        (self.out/'metrics.csv').write_text('model,mae_mw\nlast_hour,0\n')
        with self.assertRaises(ValueError):
            render_forecast.verified_inputs()


if __name__=='__main__':
    unittest.main(verbosity=2)
