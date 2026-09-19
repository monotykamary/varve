import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const root = '/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor';
const sha256 = value => createHash('sha256').update(value).digest('hex');
const stripImports = value => value.replace(/^import .*;\n/gm, '');
const before = JSON.parse(readFileSync(root + '/before.json', 'utf8'));
const roles = {
  varve: '0aff95bb-3f5a-45fe-bda0-195aa742938a',
  timescale: 'acc76115-5c5f-4be4-901d-fde4adbcbacf',
  driver: '76fdbde0-4296-46a4-a6f2-4026432fd4a3',
};
const ids = { varve: 'new-varve', timescale: 'new-timescale', driver: 'new-driver' };
const raw = { project: structuredClone(before.project), original: structuredClone(before.original) };
for (const role of Object.keys(roles)) {
  raw[role] = structuredClone(before[role].instance);
  raw[role + 'Limits'] = structuredClone(before[role].limits);
  raw[role].activeDeployments = role === 'varve' ? [] : [{ id: ids[role], status: 'SUCCESS' }];
}
const details = Object.fromEntries(Object.keys(roles).map(role => [role, {
  id: ids[role],
  status: 'SUCCESS',
  serviceId: roles[role],
  environmentId: '5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1',
  projectId: '8caffa15-0158-4822-a6c2-cb405bddc62d',
}]));
const statusBytes = readFileSync(root + '/status.mjs', 'utf8');
const campaignBytes = readFileSync(root + '/run-campaign.mjs', 'utf8');
const statusSource = stripImports(statusBytes).replace('function api(query,variables={})', 'function forbiddenOriginalApi(query,variables={})');
const outputs = [];
vm.runInNewContext(statusSource, {
  Date,
  JSON,
  process: { env: {} },
  randomUUID: () => 'probe-poll',
  console: { log: value => outputs.push(value) },
  existsSync: path => path.endsWith('/redeployed.json'),
  readFileSync(path) {
    if (path.endsWith('/before.json')) return JSON.stringify(before);
    if (path.endsWith('/activation-varve.json')) return JSON.stringify({ deploymentId: ids.varve });
    if (path.endsWith('/redeployed.json')) return JSON.stringify({ deployments: { timescale: ids.timescale, driver: ids.driver } });
    throw Error('Unexpected read ' + path);
  },
  writeFileSync() {},
  appendFileSync() {},
  unlinkSync() {},
  execFileSync() { throw Error('External execution forbidden'); },
  api(query) { return query.startsWith('query($p:') ? raw : details; },
}, { timeout: 1000 });
assert.equal(outputs.length, 1);
const statusOutput = JSON.parse(outputs[0]);
assert.deepEqual(statusOutput.readiness, { varve: 'PENDING', timescale: 'READY', driver: 'READY' });

const start = campaignBytes.indexOf('async function ready(');
const end = campaignBytes.indexOf('async function workload(', start);
assert(start >= 0 && end > start);
const readySource = campaignBytes.slice(start, end);
async function invoke(requestedRoles) {
  let slept = false;
  const result = await vm.runInNewContext(`(async()=>{${readySource}; await ready(${JSON.stringify(requestedRoles)},300); return 'returned';})()`, {
    Date,
    JSON,
    helper: async name => {
      assert.equal(name, 'status.mjs');
      return outputs[0];
    },
    event() {},
    sleep: async () => {
      slept = true;
      throw Error('probe-stop-after-first-poll');
    },
  }, { timeout: 1000 }).catch(error => error.message);
  return { result, slept };
}
const referencesOnly = await invoke(['timescale', 'driver']);
const allRoles = await invoke(['varve', 'timescale', 'driver']);
assert.deepEqual(referencesOnly, { result: 'returned', slept: false });
assert.deepEqual(allRoles, { result: 'probe-stop-after-first-poll', slept: true });
console.log(JSON.stringify({
  status_sha256: sha256(statusBytes),
  run_campaign_sha256: sha256(campaignBytes),
  actual_status_readiness: statusOutput.readiness,
  reference_ready_result: referencesOnly,
  all_roles_ready_result: allRoles,
  external_commands: 0,
  source_or_receipt_writes: 0,
}));
