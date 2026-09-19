import importlib.util, json, shutil, tempfile, unittest, hashlib
from pathlib import Path
ROOT=Path('/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor')
BASE=Path('/Users/monotykamary/VCS/working-remote/open-source/varve/docs/evidence/frontier/v10/driver')
spec=importlib.util.spec_from_file_location('installer',ROOT/'install-driver.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        self.base=self.root/'base'; self.base.mkdir()
        self.destination=self.root/'installed'
        for name in module.BASE_FILES:
            shutil.copyfile(BASE/name,self.base/name)
    def test_verified_install_is_idempotent_without_rewriting(self):
        first=module.install(self.base,self.destination)
        self.assertFalse(first['preexisting_valid_install'])
        stamps={path.name:path.stat().st_mtime_ns for path in self.destination.iterdir()}
        second=module.install(self.base,self.destination)
        self.assertTrue(second['preexisting_valid_install'])
        self.assertEqual(stamps,{path.name:path.stat().st_mtime_ns for path in self.destination.iterdir()})
        self.assertEqual(second['effective_files'],module.EFFECTIVE_FILES)
        self.assertEqual((self.base/'benchmark.py').read_bytes(),(BASE/'benchmark.py').read_bytes())
    def test_wrong_base_is_rejected_before_any_write(self):
        (self.base/'benchmark.py').write_text('wrong')
        with self.assertRaisesRegex(AssertionError,'byte mismatch'):
            module.install(self.base,self.destination)
        self.assertFalse(self.destination.exists())
    def test_corrupted_install_is_not_overwritten(self):
        module.install(self.base,self.destination)
        path=self.destination/'benchmark.py';path.chmod(0o600);path.write_text('wrong')
        with self.assertRaisesRegex(AssertionError,'byte mismatch'):
            module.install(self.base,self.destination)
        self.assertEqual(path.read_text(),'wrong')
    def test_incomplete_install_is_preserved(self):
        self.destination.mkdir()
        with self.assertRaisesRegex(AssertionError,'Incomplete'):
            module.install(self.base,self.destination)
        self.assertEqual(list(self.destination.iterdir()),[])
    def test_target_symlink_is_rejected(self):
        self.destination.symlink_to(self.base,target_is_directory=True)
        with self.assertRaisesRegex(AssertionError,'symlink'):
            module.install(self.base,self.destination)
    def test_source_symlink_is_rejected(self):
        path=self.base/'benchmark.py';path.unlink();path.symlink_to(BASE/'benchmark.py')
        with self.assertRaisesRegex(AssertionError,'file type'):
            module.install(self.base,self.destination)
        self.assertFalse(self.destination.exists())
result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromTestCase(InstallerTests))
if not result.wasSuccessful(): raise SystemExit(1)
receipt={'cases':result.testsRun,'passed':True,'external_commands':0,'infra_mutations':0,'installer_sha256':hashlib.sha256((ROOT/'install-driver.py').read_bytes()).hexdigest()}
(ROOT/'installer-tests.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
