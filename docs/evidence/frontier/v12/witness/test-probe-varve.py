import copy, hashlib, http.client, io, json, os, pwd, subprocess, time
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
ROOT=Path('/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot')
REPO=Path('/Users/monotykamary/VCS/working-remote/open-source/varve')
source=(ROOT/'probe-varve.py').read_text()
manifest=(ROOT/'stage/SOURCE_MANIFEST.json').read_bytes()
config=(ROOT/'runtime-config.json').read_bytes()
prior=json.loads((REPO/'docs/evidence/frontier/v11/runtime-varve.json').read_text())
phases=['state_lock_wait','state_lock_hold','write_prepare','wal_encode','wal_write','wal_sync','snapshot','query_build','query_wait','query_run','query_spawn','query_reset','checkpoint_prepare','checkpoint_publish','compaction_prepare','compaction_publish','disk_account','remote_io','checkpoint_capture','checkpoint_locked','root_prepare','manifest_commit','disk_lock_wait','disk_lock_hold','wal_disk_lock_wait','group_prepare','derived_verify','derived_publish','raw_verify','raw_publish','append_accounting']
cases=['valid','manifest','config','cpu','memory','phase_missing','phase_duplicate','sequence','tables','root_format','root_sequence','uid','directory','region','http_auth_failure']
for case in cases:
    runtime=copy.deepcopy(prior)
    runtime['status']['database_id']='fixture-new-db'
    runtime['initial_root']['database_id']='fixture-new-db'
    if case=='sequence': runtime['status']['sequence']=1
    if case=='root_format': runtime['initial_root']['format_version']=1
    if case=='root_sequence': runtime['initial_root']['checkpoint_sequence']=1
    names=phases[:-1] if case=='phase_missing' else phases+(['append_accounting'] if case=='phase_duplicate' else [])
    metrics=''.join('varve_phase_duration_seconds_count{phase="'+name+'"} 0\n' for name in names)
    files={'/usr/share/doc/varve/benchmark-source.json':b'{}' if case=='manifest' else manifest,'/data/probes/benchmark.json':b'{}' if case=='config' else config,'/sys/fs/cgroup/cpu.max':b'max 100000' if case=='cpu' else b'200000 100000','/sys/fs/cgroup/memory.max':b'1' if case=='memory' else b'1999998976','/data/probes/accounting-s13/manifest.bin':b'VARVEM02'+json.dumps(runtime['initial_root']).encode()+b'0'*32}
    requests=[]
    class Connection:
        def __init__(self,host,port,timeout):
            assert (host,port,timeout)==('::1',8080,20)
        def request(self,method,path,body=None,headers=None):
            assert headers['Authorization']=='Bearer fixture-only-token'
            requests.append((method,path))
            if path=='/metrics': data=metrics.encode()
            elif path=='/v1/status': data=json.dumps(runtime['status']).encode()
            elif path=='/v1/tables': data=json.dumps(['fixture'] if case=='tables' else []).encode()
            elif path=='/v1/query':
                assert json.loads(body)=={'sql':'SELECT 1 AS answer'}
                data=b'[{"answer":1}]'
            else: raise AssertionError('Unexpected HTTP path')
            self.response=SimpleNamespace(status=401 if case=='http_auth_failure' else 200,read=lambda:data)
        def getresponse(self): return self.response
        def close(self): pass
    def run(args,**kwargs):
        assert args[0]=='duckdb' and kwargs['check'] is True and kwargs['timeout']==20
        if args==['duckdb','--version']: return SimpleNamespace(stdout=prior['duckdb_version'])
        assert args[:6]==['duckdb','-no-init','-batch','-bail','-json',':memory:']
        return SimpleNamespace(stdout=json.dumps(prior['duckdb_defaults'] if 'current_setting' in args[-1] else [{'answer':1}]).encode())
    def binary(path,mode):
        assert mode=='rb' and path in ['/usr/local/bin/duckdb','/usr/local/bin/varve']
        return io.BytesIO(('fixture-'+path).encode())
    output=io.StringIO();failure=None
    env={'PATH':'/fixture','VARVE_API_TOKEN':'fixture-only-token','VARVE_DATA_DIR':'/wrong' if case=='directory' else '/data/probes/accounting-s13','RAILWAY_REPLICA_REGION':'wrong' if case=='region' else 'asia-southeast1-eqsg3a'}
    with ExitStack() as stack:
        stack.enter_context(patch.object(http.client,'HTTPConnection',Connection))
        stack.enter_context(patch.object(pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=10001,pw_gid=10001)))
        stack.enter_context(patch.object(os,'getuid',return_value=999 if case=='uid' else 10001))
        stack.enter_context(patch.dict(os.environ,env,clear=True))
        stack.enter_context(patch.object(Path,'read_bytes',lambda p:files[str(p)]))
        stack.enter_context(patch.object(Path,'read_text',lambda p:files[str(p)].decode()))
        stack.enter_context(patch.object(subprocess,'run',run))
        stack.enter_context(patch('builtins.open',binary))
        stack.enter_context(patch.object(time,'perf_counter',side_effect=iter(range(100))))
        stack.enter_context(redirect_stdout(output))
        try: exec(compile(source,'probe-varve.py','exec'),{'__name__':'__main__'})
        except AssertionError as error: failure=error
    if case=='valid':
        assert failure is None
        result=json.loads(output.getvalue())
        assert result['phase_names']==phases and ('GET','/metrics') in requests
        assert result['tables']==[] and result['config']==json.loads(config)
        assert requests.count(('POST','/v1/query'))==5
    else: assert failure is not None and output.getvalue()=='', case
receipt={'cases':len(cases),'case_names':cases,'actual_probe_source_exercised':True,'authenticated_metrics_read_verified':True,'real_http':0,'external_commands':0,'infra_mutations':0,'probe_varve_sha256':hashlib.sha256(source.encode()).hexdigest()}
(ROOT/'probe-varve-tests.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
