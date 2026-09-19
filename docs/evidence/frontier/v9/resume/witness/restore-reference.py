import hashlib, os, pwd
from pathlib import Path
user=pwd.getpwnam('benchmark')
if os.getuid()==0:
    os.setgroups([])
    os.setgid(user.pw_gid)
    os.setuid(user.pw_uid)
assert os.getuid()==10001
payload="{\"state\":\"passed\",\"namespaces\":{\"checkpoint_job\":\"vc_prefix001_921fd873\",\"compact_job\":\"vm_prefix001_921fd873\",\"pg_aggregate\":\"minute_rollup\",\"pg_receipts\":\"batch_receipts\",\"pg_schema\":\"vb_prefix001_921fd873\",\"pg_table\":\"measurements\",\"varve_aggregate\":\"va_prefix001_921fd873\",\"varve_table\":\"vb_prefix001_921fd873\"},\"manifest\":{\"total_committed_watermark_rows\":550000},\"mixed_workload\":{\"failed_or_ambiguous_rows\":0},\"resume_reference\":{\"original_report_sha256\":\"5cb8c998ea8646eaa77f916e2b4b7c0502e51fec9f229077b7e24d723ecd71bc\",\"scope\":\"Minimal recovery metadata only; full original report retained locally. No baseline load rerun.\"}}\n".encode()
assert hashlib.sha256(payload).hexdigest()=='d29ba72d930766d42a80e6f224be50ebd132f90c981fcbd7a26df4ca8023fef4'
path=Path('/results/prefix001.recovery-reference.json')
if path.exists():
    assert path.read_bytes()==payload, 'Existing reference differs; never overwrite'
else:
    os.umask(0o077)
    with path.open('xb') as output:
        output.write(payload)
print('{"baseline_recovery_reference_ready":true,"uid":10001,"baseline_rerun":false}',flush=True)
