const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { DatabaseSync } = require('node:sqlite');
const { performance } = require('node:perf_hooks');
const sqlite = new DatabaseSync(':memory:');
sqlite.exec(`CREATE TABLE emby_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE emby_nodes (id TEXT PRIMARY KEY,position INTEGER NOT NULL,data TEXT NOT NULL); CREATE INDEX emby_nodes_position ON emby_nodes(position);`);
let transferred=0, kvWrites=0;
const d1={prepare(sql){let args=[];return {bind(...a){args=a;return this;},async all(){const rows=sqlite.prepare(sql).all(...args);transferred+=JSON.stringify(rows).length;return {success:true,results:rows,meta:{changes:0}};},async first(){const rows=await this.all();return rows.results[0]||null;},async run(){const x=sqlite.prepare(sql).run(...args);return {success:true,meta:{changes:Number(x.changes)}};}};},async batch(stmts){sqlite.exec('BEGIN');try{const rows=[];for(const s of stmts)rows.push(await s.all());sqlite.exec('COMMIT');return rows;}catch(e){sqlite.exec('ROLLBACK');throw e;}}};
const source=fs.readFileSync('emby.js','utf8').replace(/\bexport\s+default\s*\{/, 'const __worker = {');
const ctx={URL,URLSearchParams,AbortController,setTimeout,clearTimeout,console,Response,Request,Headers,ReadableStream,TextEncoder,fetch:async()=>new Response('{}')};
vm.createContext(ctx);vm.runInContext(source+'\nglobalThis.worker=__worker;',ctx);const app=ctx.worker;
const now=Date.now();
const config={icons:{sample:'https://example.com/icon.svg',large:'x'.repeat(70000)},telegram:{enabled:false},logging:{enabled:false},revision:'original-revision',updatedAt:now-60000,nextScheduledCursor:0,servers:Array.from({length:70},(_,i)=>({id:i+1,name:'node-'+i,url:'https://node-'+i+'.example',fallbackUrls:[],status:'online',totalChecks:100,successfulChecks:100,uptime:'100.0',lastCheck:now-60000,history:Array.from({length:1440},(_,j)=>({status:'online',time:now-86400000+j*60000,latency:1})),mediaStats:{enabled:false}}))};
const env={EMBY_STATE_DB:d1,EMBY_DB:{get:async()=>null,put:async()=>{kvWrites++;},list:async()=>({keys:[],list_complete:true})}};
(async()=>{
 assert.equal(typeof app.runD1Scheduled,'function','D1 scheduled path must exist');
 await app.replaceD1Config(env,config);
 const original=sqlite.prepare('SELECT data FROM emby_nodes WHERE id=?').get('70').data;
 transferred=0;
 const compact=await app.d1ConfigResponse(env);const body=await compact.json();
 assert.equal(body.servers.length,70);assert.equal(body.historyMode,'paged');assert.equal(body.telegram.enabled,false);
 assert.ok(body.servers.every(s=>!s.history),'config must not return histories');
 assert.ok(transferred<150000,'config transfer must be bounded: '+transferred);
 const histories=new Map();let cursor=0;
 do {
   transferred=0;
   const page=await (await app.d1HistoryResponse(env,cursor)).json();
   assert.ok(page.servers.length<=4,'history page must be bounded');
   assert.ok(transferred<400000,'history transfer must be bounded: '+transferred);
   page.servers.forEach(s=>histories.set(String(s.id),s.history));
   cursor=page.nextCursor;
 } while(cursor!==null);
 assert.equal(histories.size,70);assert.ok([...histories.values()].every(h=>h.length===1440),'all 100800 histories must load');
 const unauthorized=await app.fetch(new Request('https://test.example/api/history'),{...env,ADMIN_TOKEN:'test'});
 assert.equal(unauthorized.status,401,'history requires admin auth');
 const badCursor=await app.fetch(new Request('https://test.example/api/history?cursor=-1',{headers:{Authorization:'Bearer test'}}),{...env,ADMIN_TOKEN:'test'});
 assert.equal(badCursor.status,400,'invalid cursors must be rejected');
 const allBefore=await app.loadConfig(env);assert.equal(allBefore.servers[0].history.length,0,'internal load must not fetch histories');
 app.probeServerRuntimeState=async(_env,s)=>({...s,totalChecks:s.totalChecks+1,lastCheck:Date.now(),history:[{status:'online',time:Date.now(),latency:0}]});
 transferred=0;const t=performance.now();const result=await app.runD1Scheduled(env);const elapsed=performance.now()-t;const scheduledBytes=transferred;
 assert.equal(result.updated,4);assert.equal(kvWrites,0);assert.ok(transferred<30000,'scheduled transfer must stay bounded: '+transferred);
 assert.equal(sqlite.prepare('SELECT data FROM emby_nodes WHERE id=?').get('70').data,original,'unprobed history must remain byte-identical');
 const a=JSON.parse(sqlite.prepare('SELECT data FROM emby_nodes WHERE id=?').get('1').data);assert.equal(a.history.length,1440);assert.equal(a.totalChecks,101);
 const state=JSON.parse(sqlite.prepare("SELECT value FROM emby_meta WHERE key='scheduler'").get().value);assert.equal(state.cursor,4);
 // Settings edits keep existing histories and do not overwrite newer runtime state.
 const edit=await app.loadConfig(env);edit.servers[0].name='renamed';edit.servers[0].lastCheck=1;edit.servers[0].totalChecks=1;await app.saveConfig(env,edit);
 const afterEdit=JSON.parse(sqlite.prepare('SELECT data FROM emby_nodes WHERE id=?').get('1').data);assert.equal(afterEdit.name,'renamed');assert.equal(afterEdit.totalChecks,101);assert.equal(afterEdit.history.length,1440);
 // URL changes during an in-flight probe must not apply the old URL's result.
 app.probeServerRuntimeState=async(_env,s)=>{sqlite.prepare("UPDATE emby_nodes SET data=json_set(data,'$.url',?) WHERE id=?").run('https://changed.example',String(s.id));return {...s,lastCheck:Date.now(),history:[{status:'offline',time:Date.now()}],status:'offline'};};
 const stale=await app.runD1Scheduled(env);assert.equal(stale.updated,0);
 // Lease prevents overlapping invocations; failures release leases and preserve cursor.
 sqlite.prepare("UPDATE emby_meta SET value=json_set(value,'$.leaseUntil',?) WHERE key='scheduler'").run(Date.now()+30000);
 assert.equal((await app.runD1Scheduled(env)).skipped,'busy');
 sqlite.prepare("UPDATE emby_meta SET value=json_set(value,'$.leaseUntil',0) WHERE key='scheduler'").run();
 app.probeServerRuntimeState=async()=>{throw Error('injected probe failure');};await assert.rejects(()=>app.runD1Scheduled(env),/injected/);
 assert.equal(JSON.parse(sqlite.prepare("SELECT value FROM emby_meta WHERE key='scheduler'").get().value).leaseUntil,0);
 // Snapshot export/import must include current D1 data, not frozen legacy KV.
 const exported=await app.exportKvSnapshot(env);const exportedConfig=JSON.parse(exported.entries.find(e=>e.key==='config').value);assert.equal(exportedConfig.servers[0].name,'renamed');assert.equal(exportedConfig.servers[0].history.length,1440);
 exportedConfig.servers[0].name='imported';await app.importKvSnapshot(env,{entries:[{key:'config',value:JSON.stringify(exportedConfig)}]},{replace:false});assert.equal((await app.loadConfig(env)).servers[0].name,'imported');
 // Notification history is aggregated in SQL, and alert markers persist only after delivery.
 sqlite.prepare("UPDATE emby_nodes SET data=json_set(data,'$.status','online','$.lastCheck',1) WHERE id='1'").run();
 sqlite.prepare("UPDATE emby_meta SET value=json_set(value,'$.cursor',0,'$.leaseUntil',0) WHERE key='scheduler'").run();
 sqlite.prepare("UPDATE emby_meta SET value=json_set(value,'$.telegram.enabled',json('true'),'$.telegram.botToken','test','$.telegram.chatId','test') WHERE key='config'").run();
 let notified=0;app.sendTelegram=async()=>{notified++;return true;};app.shouldSendOfflineAlert=()=>true;
 app.probeServerRuntimeState=async(_env,s)=>({...s,status:'offline',lastCheck:Date.now(),history:[{status:'offline',time:Date.now(),latency:0}]});
 transferred=0;await app.runD1Scheduled(env);assert.equal(notified,4);assert.ok(transferred<30000,'notifications must not fetch full histories');
 assert.ok(JSON.parse(sqlite.prepare("SELECT data FROM emby_nodes WHERE id='1'").get().data).offlineAlertSentAt>0);
 const scheduledKvWrites=kvWrites;
 assert.equal(app.isNewerRuntimeVersion('2026.05.31.4',app.APP_VERSION),false);assert.equal(app.isNewerRuntimeVersion('2099.01.01.1',app.APP_VERSION),true);
 const fetched=[];ctx.fetch=async url=>{fetched.push(url);return new Response(JSON.stringify({updateChannels:{worker:app.APP_VERSION}}));};
 const updaterEnv={...env,UPDATE_ENABLED:'1',CF_ACCOUNT_ID:'test',CF_API_TOKEN:'test',CF_WORKER_NAME:'test'};
 const checked=await app.maybeRunScheduledSelfUpdate(updaterEnv);assert.equal(checked.updated,false);assert.equal(fetched.length,1);assert.ok(fetched[0].endsWith('/src/app-meta.json'),'unchanged version must not download the Worker bundle');
 console.log(JSON.stringify({result:'passed',nodes:70,preservedHistory:100800,scheduledTransferredChars:scheduledBytes,scheduledElapsedMs:+elapsed.toFixed(2),scheduledKvWrites,tests:'history preservation, bounded batch, stale edits, URL races, lease, retry, export/import'},null,2));
})().catch(e=>{console.error(e);process.exitCode=1;});
