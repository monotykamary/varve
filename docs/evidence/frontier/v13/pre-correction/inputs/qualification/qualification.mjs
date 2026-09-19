import {readFileSync,writeFileSync,existsSync,lstatSync,readdirSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root='/tmp/varve-diagnostic.KOUrtH/retry-config/s13-qualification',repo='/Users/monotykamary/VCS/working-remote/open-source/varve';
const previousRoot=repo+'/docs/evidence/frontier/v11';
const sha=data=>createHash('sha256').update(data).digest('hex');
assert.equal(execFileSync('git',['rev-parse','--show-toplevel'],{cwd:repo,encoding:'utf8'}).trim(),repo);
const paths=[...new Set(execFileSync('git',['ls-files','-z','--cached','--others','--exclude-standard'],{cwd:repo,encoding:'utf8'}).split('\0').filter(Boolean))].filter(path=>['docs/CONFIGURATION.md','Cargo.toml','Cargo.lock','.cargo/config.toml','clients/rust/Cargo.toml','clients/rust/README.md','clients/typescript/package.json','clients/typescript/package-lock.json','clients/typescript/tsconfig.json'].includes(path)||((path.startsWith('src/')||path.startsWith('tests/')||path.startsWith('clients/rust/'))&&path.endsWith('.rs'))||((path.startsWith('clients/typescript/src/')||path.startsWith('clients/typescript/test/'))&&path.endsWith('.ts'))).sort();
const files=paths.map(path=>{assert(lstatSync(repo+'/'+path).isFile());const bytes=readFileSync(repo+'/'+path);return {path,bytes:bytes.length,sha256:sha(bytes)};});
const source_digest=sha(JSON.stringify(files));
const previous=JSON.parse(readFileSync(previousRoot+'/local-qualification.json','utf8'));
const ts=entries=>entries.filter(file=>file.path.startsWith('clients/typescript/'));
assert.deepEqual(ts(files),ts(previous.files),'Reused TypeScript unit source set changed');
if(process.argv.includes('--capture')){
 assert(!existsSync(root+'/source-before.json'),'Source capture already exists; retain previous attempt');
 writeFileSync(root+'/source-before.json',JSON.stringify({at:new Date().toISOString(),source_digest,files},null,2)+'\n',{mode:0o600});
 console.log(JSON.stringify({captured:true,source_digest,files:files.length,tests_run:false}));
}else{
 const before=JSON.parse(readFileSync(root+'/source-before.json','utf8'));
 assert.equal(before.source_digest,source_digest,'Source changed during qualification');
 assert.deepEqual(before.files,files);
 const fullPath=root+'/qualification.log',unitPath=previousRoot+'/typescript-unit.log';
 const text=readFileSync(fullPath,'utf8'),unit=readFileSync(unitPath,'utf8');
 assert(text.includes('ALL_REQUESTED_GATES_PASSED'));
 assert(text.includes('GATE strict workspace default-feature lint'));
 assert(unit.includes('pass 18'));
 const covered=new Set();let target='',passed=0,ignored=0;
 for(const line of text.split('\n')){
  if(line.trimStart().startsWith('Running '))target=line.trim().split(' ')[1];
  if(line.startsWith('test result: ok.')&&line.includes('; 0 filtered out;')){const fields=line.split(' ').filter(Boolean);passed+=Number(fields[3]);ignored+=Number(fields[7]);covered.add(target);}
 }
 for(const name of readdirSync(repo+'/tests').filter(name=>name.endsWith('.rs')))assert(covered.has('tests/'+name),'Unverified target '+name);
 for(const gate of ['test engine::engine_metrics_tests::group_prepare_is_closed_at_existing_publication_and_checkpoint_hooks ... ok','test engine::engine_metrics_tests::actual_checkpoint_modes_attribute_new_raw_and_derived_objects ... ok','test retained_storage_proof_omits_only_inaccessible_catalog_rows ... ok','test selected_rollup_refreshes_only_for_relevant_exact_payload_changes ... ok','test retained_query_metrics_witness_reuse_and_only_new_raw_rows ... ok','test metrics::tests::phase_indices_and_names_preserve_the_original_twenty_two ... ok','test real_service_auth_writes_query_errors_deadline_disconnect_and_close ... ok','test every_public_configuration_entry_is_documented ... ok','test frozen_prefix_checkpoints_are_explicitly_opted_in ... ok','test metrics::tests::every_phase_is_registered_observed_and_exported_exactly_once ... ok','pass 1'])assert(text.includes(gate),'Missing gate '+gate);
 for(const gate of ['test engine::append_accounting_tests::projection_matches_legacy_and_full_maps_with_exact_call_counts ... ok','test engine::append_accounting_tests::touched_key_new_and_previous_serializers_each_run_once ... ok','test engine::append_accounting_tests::metadata_headroom_precedes_deferred_derived_errors ... ok','test engine::append_accounting_tests::direct_reprojects_after_prepare_record_prunes_at_same_sequence ... ok','test engine::append_accounting_tests::append_accounting_phase_covers_direct_group_and_open_replay_not_retries ... ok','test engine::append_accounting_tests::direct_wal_failure_precedes_deferred_derived_accounting_failure ... ok'])assert(text.includes(gate),'Missing S13 gate '+gate);
 assert.equal(passed,410,'S13 expected full Rust count');assert.equal(ignored,1,'Only the documented live-S3 test is ignored');
 const logs=[fullPath,unitPath].map(path=>({path,sha256:sha(readFileSync(path))}));
 const reportPath=root+'/qualification.json';
 if(process.argv.includes('--check')){
  const saved=JSON.parse(readFileSync(reportPath,'utf8'));
  assert.equal(saved.source_digest,source_digest);assert.deepEqual(saved.logs,logs);
 }else{
  assert(!existsSync(reportPath),'Qualification already frozen');
  writeFileSync(reportPath,JSON.stringify({at:new Date().toISOString(),source_digest,files,logs,rust_passed:passed,ignored,typescript_unit:18,typescript_real_service:1,format:true,strict_workspace_clippy:true,live_s3:false,performance_qualified:false,source_unchanged_across_checks:true,note:'Full Rust library/binary/integration/doc gates and real clients rerun on the same pre/post source capture, including compile-time configuration documentation. Examples compiled, not executed. Prior TS unit log reused only after exact complete selected TS source/config set identity.'},null,2)+'\n',{mode:0o600});
 }
 console.log(JSON.stringify({source_digest,source_files:files.length,rust_passed:passed,ignored,typescript_unit:18,typescript_real_service:1,source_unchanged_across_checks:true,live_s3:false,performance_qualified:false}));
}
