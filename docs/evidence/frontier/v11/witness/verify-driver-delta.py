import ast
import hashlib
import json
from pathlib import Path
root = Path(__file__).resolve().parents[1]
repo = Path('/Users/monotykamary/VCS/working-remote/open-source/varve')
old_root = root / 'base-driver'
new_root = root / 'driver'
old_source = (old_root / 'benchmark.py').read_text()
new_source = (new_root / 'benchmark.py').read_text()
old = ast.parse(old_source)
new = ast.parse(new_source)
def named(module, name):
    return next(node for node in module.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
new.body[new.body.index(named(new, 'redactor'))] = named(old, 'redactor')
mixed = named(new, 'mixed_workload')
failed = next(node for node in mixed.body if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == 'failures')
assert len(failed.body) == 2
partial, raised = failed.body
assert isinstance(partial, ast.Assign) and isinstance(partial.value, ast.Dict)
assert isinstance(raised, ast.Raise)
assert ast.dump(raised.cause) == ast.dump(ast.parse('failures[0]', mode='eval').body)
raised.cause = None
normal = next(node for node in mixed.body if isinstance(node, ast.Return)).value
normal_values = {key.value: value for key, value in zip(normal.keys, normal.values)}
extra = {'generation_encoding_ms', 'varve_concurrent_read_ms', 'timescale_concurrent_read_ms', 'concurrent_read_pair_ms'}
remaining = []
seen = set()
for key, value in zip(partial.value.keys, partial.value.values):
    if key.value in extra:
        assert ast.dump(value) == ast.dump(normal_values[key.value])
        seen.add(key.value)
    else:
        remaining.append((key, value))
assert seen == extra
partial.value.keys = [key for key, _ in remaining]
partial.value.values = [value for _, value in remaining]
assert ast.dump(old) == ast.dump(new), 'Changes outside redactor, failure sample retention or explicit causal chain'
sha = lambda data: hashlib.sha256(data).hexdigest()
for name in ('core.py', 'Dockerfile', 'requirements.txt'):
    assert (old_root / name).read_bytes() == (new_root / name).read_bytes()
files = {name: sha((new_root / name).read_bytes()) for name in ('benchmark.py', 'core.py', 'Dockerfile', 'requirements.txt', 'tests/test_core.py', 'tests/test_runner.py', 'README.md')}
scope = json.loads((root / 'driver-scope.json').read_text())
assert files == scope['files']
assert sha(old_source.encode()) == scope['baseline_driver_sha256']
assert sha(new_source.encode()) == scope['effective_driver_sha256']
log = (root / 'driver-diagnostic-tests.log').read_bytes()
assert sha(log) == scope['test_log_sha256']
assert b'Ran 21 tests' in log and log.rstrip().endswith(b'OK')
print(json.dumps({'normal_workload_ast_unchanged': True, 'driver_files_verified': len(files), 'offline_tests_recorded': 21}))
