import hashlib, http.client, json, os, pwd, subprocess, tempfile, time
from pathlib import Path
user=pwd.getpwnam('varve')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001
connection=http.client.HTTPConnection('::1',8080,timeout=20)
def api(method,path,body=None):
    headers={'Authorization':'Bearer '+os.environ['VARVE_API_TOKEN']}
    if body is not None:
        headers['Content-Type']='application/json'
        body=json.dumps(body).encode()
    connection.request(method,path,body=body,headers=headers)
    response=connection.getresponse()
    result=json.loads(response.read())
    assert response.status==200, (response.status,result)
    return result
manifest=Path('/usr/share/doc/varve/benchmark-source.json').read_bytes()
result={'event':'frontier_varve_preflight','uid':os.getuid(),'region':os.environ.get('RAILWAY_REPLICA_REGION'),'data_dir':os.environ.get('VARVE_DATA_DIR'),'cpu_max':Path('/sys/fs/cgroup/cpu.max').read_text().strip(),'memory_max':Path('/sys/fs/cgroup/memory.max').read_text().strip(),'source_manifest_sha256':hashlib.sha256(manifest).hexdigest(),'config_sha256':hashlib.sha256(Path('/data/probes/benchmark.json').read_bytes()).hexdigest(),'status':api('GET','/v1/status')}
assert result['source_manifest_sha256']=='ea15372e2de15f3e1db739d02e339b4ab46270332e0121dffdc2b69f644101bc'
assert result['config_sha256']=='51aaee9c4238a933dcdb964e4877cec3296456ab92f4698fa0eafe78a8977dc7'
assert result['region']=='asia-southeast1-eqsg3a'
assert result['data_dir']=='/data/probes/reuse-20260916-k2exip'
assert result['cpu_max']=='200000 100000' and int(result['memory_max'])==1999998976
assert api('GET','/v1/tables')==[]
root_bytes=Path(result['data_dir'],'manifest.bin').read_bytes()
assert root_bytes[:8]==b'VARVEM02'
result['initial_root']=json.loads(root_bytes[8:-32])
assert result['initial_root']['format_version']==2 and result['initial_root']['checkpoint_sequence']==0
assert result['status']['sequence']==0
config=json.loads(Path('/data/probes/benchmark.json').read_text())
assert config['derived_pages'] is True and config['derived_max_bytes']==268435456
assert result['status']['hot_rows']==0 and result['status']['fenced'] is None
with open('/usr/local/bin/varve','rb') as binary:
    result['binary_sha256']=hashlib.file_digest(binary,'sha256').hexdigest()
with tempfile.TemporaryDirectory(prefix='frontier-diagnostic-') as worker:
    env={'PATH':os.environ['PATH'],'HOME':worker,'TMPDIR':worker,'TMP':worker,'TEMP':worker}
    base=['duckdb','-no-init','-batch','-bail','-json',':memory:','-c']
    result['duckdb_defaults']=json.loads(subprocess.run(base+["SELECT current_setting('threads') AS threads, current_setting('memory_limit') AS memory_limit"],env=env,capture_output=True,check=True,timeout=20).stdout)
    result['diagnostic_raw_ms']={}
    for name,sql in [('bare_select','SELECT 1 AS answer'),('configured_select',"SET threads=2; SET memory_limit='256MB'; SELECT 1 AS answer")]:
        samples=[]
        for _ in range(5):
            start=time.perf_counter()
            answer=subprocess.run(base+[sql],env=env,capture_output=True,check=True,timeout=20).stdout
            samples.append((time.perf_counter()-start)*1000)
            assert json.loads(answer)==[{'answer':1}]
        result['diagnostic_raw_ms'][name]=samples
    samples=[]
    for _ in range(5):
        start=time.perf_counter()
        assert api('POST','/v1/query',{'sql':'SELECT 1 AS answer'})==[{'answer':1}]
        samples.append((time.perf_counter()-start)*1000)
    result['diagnostic_raw_ms']['http_select']=samples
result['diagnostic_scope']='five local-to-container observations per case before fixture creation; not a workload benchmark or tail estimate'
connection.close()
print(json.dumps(result),flush=True)
