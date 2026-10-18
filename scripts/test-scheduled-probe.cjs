const fs = require('fs');
const vm = require('vm');

const fail = (message) => {
  console.error(`scheduled probe test failed: ${message}`);
  process.exit(1);
};

const source = fs.readFileSync('emby.js', 'utf8');
const workerSource = source.replace(/\bexport\s+default\s*\{/, 'const __worker = {').replace(/\n\};\s*$/, '\n};\n');
const context = { URL, URLSearchParams, AbortController, setTimeout, clearTimeout, console, fetch: async () => new Response('{}') };
vm.createContext(context);
vm.runInContext(`${workerSource}\nglobalThis.worker = __worker;`, context);

const server = (id) => ({
  id,
  name: `server-${id}`,
  url: `https://server-${id}.example`,
  fallbackUrls: [],
  status: 'online',
  totalChecks: 100,
  successfulChecks: 100,
  uptime: '100.0',
  latency: 10,
  lastCheck: Date.now() - 60000,
  offlineSince: 0,
  offlineAlertSentAt: 0,
  consecutiveFailures: 0,
  firstFailureAt: 0,
  addressProbeResults: [],
  history: Array.from({ length: 1440 }, (_, index) => ({ status: 'online', time: Date.now() - index * 60000, latency: 10 })),
  mediaStats: { enabled: false }
});

(async () => {
  const config = { servers: [server('a'), server('b'), server('c'), server('d'), server('e')], icons: {}, telegram: { enabled: false }, logging: { enabled: false }, updatedAt: 1, nextScheduledCursor: 0 };
  let saved;
  let putCount = 0;
  const env = { EMBY_DB: { get: async () => JSON.stringify(config), put: async (key, value) => { if (key !== 'config') fail(`unexpected key ${key}`); saved = JSON.parse(value); putCount += 1; } } };

  const app = context.worker;
  app.loadConfig = async (_env, options = {}) => options.skipHistoryNormalization ? structuredClone(config) : structuredClone(config);
  app.probeServerRuntimeState = async (_env, input, _forceMedia, options) => {
    if (!options.skipHistoryNormalization || !options.skipMediaRefresh) fail('scheduled probe flags were not set');
    const history = [...input.history, { status: 'offline', time: Date.now(), latency: 0 }];
    if (history.length > app.HISTORY_LIMIT) history.splice(0, history.length - app.HISTORY_LIMIT);
    return { ...input, status: 'offline', totalChecks: input.totalChecks + 1, latency: 0, lastCheck: Date.now(), consecutiveFailures: 3, offlineSince: Date.now() - 300000, history, previousStatus: 'online' };
  };

  const result = await app.runProbeLogic(env, config, { source: 'scheduled', statusOnly: true });
  if (putCount !== 1) fail(`expected one KV write, got ${putCount}`);
  if (result.servers.filter((item) => item.status === 'offline').length !== 4) fail('the first four servers were not updated');
  if (result.servers[4].status !== 'online' || result.servers[4].history.length !== 1440) fail('an unprobed server was modified');
  if (result.servers[0].history.length !== 1440 || result.servers[0].history.at(-1).status !== 'offline') fail('probed history was not appended and bounded');
  if (result.nextScheduledCursor !== 4 || result.nextCursor !== 4 || result.hasMore !== true) fail('cursor was not advanced');
  if (saved.servers[4].totalChecks !== 100) fail('unprobed server data was not preserved in KV');
  console.log('scheduled probe test passed: one batch updated, other servers and histories preserved');
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
