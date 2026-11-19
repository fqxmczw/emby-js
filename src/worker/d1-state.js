/* Incremental state. The legacy KV config remains an untouched rollback snapshot. */
  usesD1State(env) {
      return Boolean(env && env.EMBY_STATE_DB && env.RUNTIME_ENV !== 'docker');
  },

  D1_RUNTIME_FIELDS: ['status', 'totalChecks', 'successfulChecks', 'uptime', 'latency', 'lastCheck', 'offlineSince', 'offlineAlertSentAt', 'consecutiveFailures', 'firstFailureAt', 'addressProbeResults'],

  async loadD1Config(env, options = {}) {
      const db = env.EMBY_STATE_DB;
      const results = await db.batch([
          db.prepare("SELECT value FROM emby_meta WHERE key='config'"),
          db.prepare(`SELECT ${options.includeHistory ? 'data' : "json_remove(data,'$.history') AS data"} FROM emby_nodes ORDER BY position`)
      ]);
      if (!results[0].results[0]) throw new Error('D1 state is not initialized; migrate the existing KV snapshot first');
      const config = JSON.parse(results[0].results[0].value);
      config.servers = results[1].results.map(row => ({ ...JSON.parse(row.data), ...(options.includeHistory ? {} : { history: [] }) }));
      return config;
  },

  async replaceD1Config(env, config) {
      if (!config || !Array.isArray(config.servers)) throw new Error('Invalid config snapshot');
      const ids = config.servers.map(s => String(s.id));
      if (new Set(ids).size !== ids.length) throw new Error('Duplicate server IDs');
      const meta = { ...config };
      delete meta.servers;
      const db = env.EMBY_STATE_DB;
      const statements = [db.prepare('DELETE FROM emby_nodes')];
      config.servers.forEach((server, index) => statements.push(db.prepare('INSERT INTO emby_nodes(id,position,data) VALUES(?,?,?)').bind(String(server.id), index, JSON.stringify(server))));
      statements.push(db.prepare("INSERT INTO emby_meta(key,value) VALUES('config',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(JSON.stringify(meta)));
      statements.push(db.prepare("INSERT INTO emby_meta(key,value) VALUES('scheduler',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(JSON.stringify({ cursor: Number(config.nextScheduledCursor) || 0, leaseUntil: 0 })));
      await db.batch(statements);
      return config;
  },

  async saveD1Config(env, config, options = {}) {
      if (options.replaceHistory) return this.replaceD1Config(env, config);
      const clean = options.raw ? config : this.sanitizeConfig(config, { skipHistoryNormalization: true });
      if (new Set(clean.servers.map(s => String(s.id))).size !== clean.servers.length) throw new Error('Duplicate server IDs');
      const current = await this.loadD1Config(env);
      const meta = { ...current, ...clean };
      delete meta.servers;
      // Hash settings once on edits, never on a scheduled status update.
      meta.revision = this.configRevision({ ...meta, servers: clean.servers });
      const db = env.EMBY_STATE_DB;
      const statements = [];
      const ids = new Set(clean.servers.map(s => String(s.id)));
      for (const server of current.servers) if (!ids.has(String(server.id))) statements.push(db.prepare('DELETE FROM emby_nodes WHERE id=?').bind(String(server.id)));
      clean.servers.forEach((server, index) => {
          const settings = { ...server };
          const runtime = {};
          for (const field of this.D1_RUNTIME_FIELDS) { if (server[field] !== undefined) runtime[field] = server[field]; delete settings[field]; }
          delete settings.history;
          const point = (Array.isArray(server.history) ? server.history : []).at(-1);
          const history = Array.isArray(server.history) ? server.history : [];
          statements.push(db.prepare(`INSERT INTO emby_nodes(id,position,data) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET position=excluded.position,data=json_set(json_patch(json_patch(emby_nodes.data,json(?)),CASE WHEN COALESCE(json_extract(emby_nodes.data,'$.lastCheck'),0)<=? THEN json(?) ELSE '{}' END),'$.history',json(CASE WHEN ? IS NOT NULL AND COALESCE(json_extract(emby_nodes.data,'$.lastCheck'),0)<? THEN json_insert(CASE WHEN json_array_length(emby_nodes.data,'$.history')>=? THEN json_remove(json_extract(emby_nodes.data,'$.history'),'$[0]') ELSE COALESCE(json_extract(emby_nodes.data,'$.history'),'[]') END,'$[#]',json(?)) ELSE COALESCE(json_extract(emby_nodes.data,'$.history'),'[]') END))`).bind(String(server.id), index, JSON.stringify({ ...server, history }), JSON.stringify(settings), Number(server.lastCheck) || 0, JSON.stringify(runtime), point ? JSON.stringify(point) : null, Number(server.lastCheck) || 0, this.HISTORY_LIMIT, point ? JSON.stringify(point) : null));
      });
      statements.push(db.prepare("UPDATE emby_meta SET value=? WHERE key='config'").bind(JSON.stringify(meta)));
      await db.batch(statements);
      return { ...meta, servers: clean.servers };
  },

  async d1ConfigResponse(env) {
      // D1 returns node JSON strings. Stream them without parsing/stringifying all histories in the Worker.
      const db = env.EMBY_STATE_DB;
      const results = await db.batch([db.prepare("SELECT value FROM emby_meta WHERE key='config'"), db.prepare('SELECT data FROM emby_nodes ORDER BY position')]);
      if (!results[0].results[0]) throw new Error('D1 state not initialized');
      const config = JSON.parse(results[0].results[0].value);
      const header = { ...config, storageMode: 'd1', notifyEnabled: this.isTelegramEnabled(env, config), telegram: this.getTelegramConfig(env, config), publicShareBaseUrl: this.getPublicShareBaseUrl(env), publicShareWildcardDomain: this.getPublicShareWildcardDomain(env) };
      const encoder = new TextEncoder();
      const chunks = [JSON.stringify(header).slice(0, -1) + ',"servers":[', ...results[1].results.map((row, i) => (i ? ',' : '') + row.data), ']}'];
      return new Response(new ReadableStream({ pull(controller) { if (chunks.length) controller.enqueue(encoder.encode(chunks.shift())); else controller.close(); } }), { headers: { 'Content-Type': 'application/json;charset=utf-8', 'Cache-Control': 'no-store' } });
  },

  async runD1Scheduled(env) {
      const db = env.EMBY_STATE_DB;
      const leaseUntil = Date.now() + 90000;
      const claim = await db.prepare("UPDATE emby_meta SET value=json_set(value,'$.leaseUntil',?) WHERE key='scheduler' AND COALESCE(json_extract(value,'$.leaseUntil'),0)<? RETURNING value").bind(leaseUntil, Date.now()).first();
      if (!claim) return { skipped: 'busy' };
      try {
          const cursor = Number(JSON.parse(claim.value).cursor) || 0;
          const results = await db.batch([
              db.prepare("SELECT value FROM emby_meta WHERE key='config'"),
              db.prepare("SELECT json_remove(data,'$.history') AS data FROM emby_nodes ORDER BY position LIMIT 4 OFFSET ?").bind(cursor),
              db.prepare('SELECT COUNT(*) AS count FROM emby_nodes')
          ]);
          const config = JSON.parse(results[0].results[0].value);
          const total = results[2].results[0].count;
          const targets = results[1].results.map(row => ({ ...JSON.parse(row.data), history: [] }));
          const probed = await this.mapWithConcurrency(targets, 4, server => this.probeServerRuntimeState(env, server, false, { skipHistoryNormalization: true, skipMediaRefresh: true, recordLatency: false, flow: 'scheduled', logConfig: config }));
          const statements = [];
          const notify = [];
          for (let i = 0; i < probed.length; i += 1) {
              const server = probed[i];
              const before = targets[i];
              const runtime = {};
              for (const field of this.D1_RUNTIME_FIELDS) if (server[field] !== undefined) runtime[field] = server[field];
              const point = server.history.at(-1);
              statements.push(db.prepare(`UPDATE emby_nodes SET data=json_set(json_patch(data,json(?)),'$.history',json(CASE WHEN ? IS NULL THEN COALESCE(json_extract(data,'$.history'),'[]') ELSE json_insert(CASE WHEN json_array_length(data,'$.history')>=? THEN json_remove(json_extract(data,'$.history'),'$[0]') ELSE COALESCE(json_extract(data,'$.history'),'[]') END,'$[#]',json(?)) END)) WHERE id=? AND json_extract(data,'$.url')=? AND COALESCE(json_extract(data,'$.lastCheck'),0)<? AND COALESCE(json_extract(data,'$.fallbackUrls'),'[]')=? RETURNING id`).bind(JSON.stringify(runtime), point ? JSON.stringify(point) : null, this.HISTORY_LIMIT, point ? JSON.stringify(point) : null, String(server.id), before.url, Number(server.lastCheck) || 0, JSON.stringify(before.fallbackUrls || [])));
              if (this.isTelegramEnabled(env, config) && ((server.status === 'offline' && this.shouldSendOfflineAlert(server)) || (before.status === 'offline' && server.status === 'online' && before.offlineAlertSentAt))) notify.push({ index: i, server, before });
          }
          const nextCursor = cursor + targets.length < total ? cursor + targets.length : 0;
          statements.push(db.prepare("UPDATE emby_meta SET value=json_set(value,'$.cursor',?,'$.leaseUntil',0) WHERE key='scheduler' AND json_extract(value,'$.leaseUntil')=?").bind(nextCursor, leaseUntil));
          statements.push(db.prepare("UPDATE emby_meta SET value=json_set(value,'$.updatedAt',?,'$.nextScheduledCursor',?) WHERE key='config'").bind(Date.now(), nextCursor));
          const saved = await db.batch(statements);
          for (const item of notify) {
              if (!saved[item.index].results.length) continue;
              // Aggregate this node's history inside D1; notification spikes must not load it into the Worker.
              const row = await db.prepare("SELECT json_remove(data,'$.history') AS data FROM emby_nodes WHERE id=?").bind(String(item.server.id)).first();
              if (!row) continue;
              const current = JSON.parse(row.data);
              const stats = await db.prepare(`WITH points AS (SELECT json_extract(h.value,'$.status') AS status, json_extract(h.value,'$.latency') AS latency, LAG(json_extract(h.value,'$.status')) OVER (ORDER BY CAST(h.key AS INTEGER)) AS previous FROM emby_nodes n,json_each(n.data,'$.history') h WHERE n.id=? AND json_extract(h.value,'$.time')>=?) SELECT COUNT(*) AS total, COALESCE(SUM(status='online'),0) AS online, COALESCE(SUM(status<>'online'),0) AS offline, COALESCE(SUM(status='offline' AND (previous IS NULL OR previous<>'offline')),0) AS offlineEvents, COALESCE(AVG(CASE WHEN status='online' AND latency>0 THEN latency END),0) AS avgLatency FROM points`).bind(String(current.id), Date.now() - 86400000).first();
              current._recentHistoryStats = { ...stats, uptime: stats.total ? ((stats.online / stats.total) * 100).toFixed(1) : '---', avgLatency: Math.round(stats.avgLatency) };
              if (Number(current.lastCheck) !== Number(item.server.lastCheck) || current.url !== item.server.url) continue;
              const message = this.buildStatusMessage({ ...current, offlineSince: item.before.offlineSince || current.offlineSince }, item.before.status, current.status);
              if (await this.sendTelegram(env, message, config)) await db.prepare("UPDATE emby_nodes SET data=json_set(data,'$.offlineAlertSentAt',?) WHERE id=? AND json_extract(data,'$.lastCheck')=?").bind(current.status === 'offline' ? current.lastCheck : 0, String(current.id), current.lastCheck).run();
          }
          return { updated: saved.slice(0, targets.length).filter(x => x.results.length).length, nextCursor, total };
      } finally {
          await db.prepare("UPDATE emby_meta SET value=json_set(value,'$.leaseUntil',0) WHERE key='scheduler' AND json_extract(value,'$.leaseUntil')=?").bind(leaseUntil).run();
      }
  },
