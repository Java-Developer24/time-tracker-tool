require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { BigQuery } = require('@google-cloud/bigquery');
const { WebSocketServer } = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const API_KEY = process.env.API_KEY;
const PORT = parseInt(process.env.PORT || '3000', 10);
const VERBOSE_LOGS = process.env.VERBOSE_LOGS === 'true';

const BQ_PROJECT_ID = process.env.BQ_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || '';
const BQ_LOCATION = process.env.BQ_LOCATION || 'US';
const BQ_DATASET = process.env.BQ_DATASET || 'timetracker';
const BQ_ACTIVITY_TABLE = process.env.BQ_ACTIVITY_TABLE || 'activity_logs';
const BQ_STATUS_TABLE = process.env.BQ_STATUS_TABLE || 'agent_status';
const BQ_DAILY_SUMMARY_VIEW = process.env.BQ_DAILY_SUMMARY_VIEW || 'daily_summary';

const OHR_SHEET_ID = process.env.OHR_SHEET_ID || '1KlfU6Juc2vlErxgRoDEGLq6trNIyjqUNMjmr3q7K6vM';
const OHR_SHEET_GID = process.env.OHR_SHEET_GID || '0';
const OHR_SHEET_CSV_URL = process.env.OHR_SHEET_CSV_URL || `https://docs.google.com/spreadsheets/d/${OHR_SHEET_ID}/export?format=csv&gid=${OHR_SHEET_GID}`;
const OHR_TIMESTAMP_WEBHOOK_URL = process.env.OHR_TIMESTAMP_WEBHOOK_URL || '';
const ALLOW_INSECURE_TLS = process.env.ALLOW_INSECURE_TLS === 'true';

function getBigQueryCredentialsFromEnv() {
  const rawJson = (process.env.GOOGLE_CREDENTIALS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (parsed.private_key) {
        parsed.private_key = String(parsed.private_key).replace(/\\n/g, '\n');
      }
      return {
        credentials: parsed,
        source: 'GOOGLE_CREDENTIALS_JSON'
      };
    } catch (err) {
      throw new Error(`Invalid GOOGLE_CREDENTIALS_JSON: ${err.message}`);
    }
  }

  const clientEmail = (process.env.GOOGLE_CLIENT_EMAIL || '').trim();
  const privateKey = process.env.GOOGLE_PRIVATE_KEY || '';
  if (clientEmail || privateKey) {
    if (!clientEmail || !privateKey) {
      throw new Error('Both GOOGLE_CLIENT_EMAIL and GOOGLE_PRIVATE_KEY are required for env-based BigQuery auth.');
    }
    return {
      credentials: {
        client_email: clientEmail,
        private_key: privateKey.replace(/\\n/g, '\n')
      },
      source: 'GOOGLE_CLIENT_EMAIL/GOOGLE_PRIVATE_KEY'
    };
  }

  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return { credentials: null, source: 'GOOGLE_APPLICATION_CREDENTIALS (file path)' };
  }
  return { credentials: null, source: 'Application Default Credentials' };
}

const { credentials: bigQueryCredentials, source: bigQueryAuthSource } = getBigQueryCredentialsFromEnv();

const bigquery = new BigQuery({
  projectId: BQ_PROJECT_ID || undefined,
  credentials: bigQueryCredentials || undefined
});

function logInfo(message, meta) {
  if (meta !== undefined) {
    console.log(message, meta);
    return;
  }
  console.log(message);
}

function logWarn(message, meta) {
  if (meta !== undefined) {
    console.warn(message, meta);
    return;
  }
  console.warn(message);
}

function logError(message, meta) {
  if (meta !== undefined) {
    console.error(message, meta);
    return;
  }
  console.error(message);
}

function logVerbose(message, meta) {
  if (!VERBOSE_LOGS) return;
  logInfo(message, meta);
}

function getProjectId() {
  const projectId = BQ_PROJECT_ID || bigquery.projectId;
  if (!projectId) {
    throw new Error('Missing BQ project id. Set BQ_PROJECT_ID or GOOGLE_CLOUD_PROJECT.');
  }
  return projectId;
}

function tableRef(tableName) {
  return `\`${getProjectId()}.${BQ_DATASET}.${tableName}\``;
}

function viewRef(viewName) {
  return `\`${getProjectId()}.${BQ_DATASET}.${viewName}\``;
}

function nowISO() {
  return new Date().toISOString();
}

function parseTimestamp(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeOhrId(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeOptionalString(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  if (!key || key !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

function splitCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }

  result.push(current);
  return result.map((value) => value.trim());
}

function parseAgentCSV(csvText) {
  const lines = (csvText || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (lines.length < 2) {
    return [];
  }

  const headers = splitCSVLine(lines[0]).map((h) => h.replace(/^"|"$/g, '').toLowerCase());
  const ohrIndex = headers.indexOf('agent_ohr');
  const nameIndex = headers.indexOf('agent_name');
  const idIndex = headers.indexOf('id');
  const supervisorIndex = headers.indexOf('supervisor');
  const departmentIndex = headers.indexOf('department');

  if (ohrIndex === -1 || nameIndex === -1) {
    throw new Error('Google Sheet must include agent_ohr and agent_name columns');
  }

  return lines.slice(1).map((line) => {
    const cols = splitCSVLine(line).map((v) => v.replace(/^"|"$/g, ''));
    return {
      id: cols[idIndex] || '',
      agent_ohr: cols[ohrIndex] || '',
      agent_name: cols[nameIndex] || '',
      supervisor: supervisorIndex >= 0 ? (cols[supervisorIndex] || '') : '',
      department: departmentIndex >= 0 ? (cols[departmentIndex] || '') : ''
    };
  });
}

function httpRequest(url, options = {}) {
  const requestUrl = new URL(url);
  const transport = requestUrl.protocol === 'http:' ? http : https;
  const timeoutMs = options.timeoutMs || 10000;

  return new Promise((resolve, reject) => {
    const req = transport.request(
      {
        protocol: requestUrl.protocol,
        hostname: requestUrl.hostname,
        port: requestUrl.port,
        path: `${requestUrl.pathname}${requestUrl.search}`,
        method: options.method || 'GET',
        headers: options.headers || {},
        rejectUnauthorized: !ALLOW_INSECURE_TLS
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('end', async () => {
          const text = Buffer.concat(chunks).toString('utf8');

          if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            try {
              const redirectUrl = new URL(res.headers.location, requestUrl.toString()).toString();
              const redirected = await httpRequest(redirectUrl, options);
              resolve(redirected);
              return;
            } catch (err) {
              reject(err);
              return;
            }
          }

          resolve({
            statusCode: res.statusCode || 0,
            headers: res.headers,
            text
          });
        });
      }
    );

    req.on('error', reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error('HTTP request timeout')));

    if (options.body) {
      req.write(options.body);
    }

    req.end();
  });
}

async function fetchSheetText(url) {
  const response = await httpRequest(url);
  if (response.statusCode < 200 || response.statusCode >= 300) {
    throw new Error(`Sheet fetch failed with status ${response.statusCode}`);
  }
  return response.text;
}

async function tryWriteTimestampToWebhook(payload) {
  if (!OHR_TIMESTAMP_WEBHOOK_URL) {
    return { logged: false, status: 'not_configured' };
  }

  try {
    const body = JSON.stringify(payload);
    const response = await httpRequest(OHR_TIMESTAMP_WEBHOOK_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      },
      body
    });

    if (response.statusCode >= 200 && response.statusCode < 300) {
      return { logged: true, status: 'ok' };
    }

    return {
      logged: false,
      status: `webhook_http_${response.statusCode}`
    };
  } catch (err) {
    return {
      logged: false,
      status: `webhook_error:${err.message}`
    };
  }
}

async function ensureBigQueryObjects() {
  logInfo(`[BQ] Ensuring dataset/tables/views in ${getProjectId()}.${BQ_DATASET} (${BQ_LOCATION})`);
  const dataset = bigquery.dataset(BQ_DATASET);
  const [datasetExists] = await dataset.exists();
  if (!datasetExists) {
    await dataset.create({ location: BQ_LOCATION });
    logInfo(`[BQ] Created dataset: ${getProjectId()}.${BQ_DATASET}`);
  } else {
    logVerbose(`[BQ] Dataset exists: ${getProjectId()}.${BQ_DATASET}`);
  }

  const activityTable = dataset.table(BQ_ACTIVITY_TABLE);
  const [activityExists] = await activityTable.exists();
  if (!activityExists) {
    await activityTable.create({
      schema: [
        { name: 'id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'agent_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'activity_type', type: 'STRING', mode: 'REQUIRED' },
        { name: 'sub_category', type: 'STRING' },
        { name: 'url', type: 'STRING' },
        { name: 'page_title', type: 'STRING' },
        { name: 'additional_info', type: 'STRING' },
        { name: 'start_time', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'end_time', type: 'TIMESTAMP' },
        { name: 'duration_seconds', type: 'INT64' },
        { name: 'manually_categorized', type: 'BOOL' },
        { name: 'logged_at', type: 'TIMESTAMP' }
      ],
      timePartitioning: { type: 'DAY', field: 'start_time' },
      clustering: { fields: ['agent_id', 'activity_type'] }
    });
    logInfo(`[BQ] Created table: ${getProjectId()}.${BQ_DATASET}.${BQ_ACTIVITY_TABLE}`);
  } else {
    logVerbose(`[BQ] Table exists: ${getProjectId()}.${BQ_DATASET}.${BQ_ACTIVITY_TABLE}`);
  }

  const statusTable = dataset.table(BQ_STATUS_TABLE);
  const [statusExists] = await statusTable.exists();
  if (!statusExists) {
    await statusTable.create({
      schema: [
        { name: 'agent_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'current_status', type: 'STRING' },
        { name: 'current_activity', type: 'STRING' },
        { name: 'current_url', type: 'STRING' },
        { name: 'status_updated_at', type: 'TIMESTAMP' },
        { name: 'last_seen', type: 'TIMESTAMP' }
      ]
    });
    logInfo(`[BQ] Created table: ${getProjectId()}.${BQ_DATASET}.${BQ_STATUS_TABLE}`);
  } else {
    logVerbose(`[BQ] Table exists: ${getProjectId()}.${BQ_DATASET}.${BQ_STATUS_TABLE}`);
  }

  const viewQuery = `
    CREATE OR REPLACE VIEW ${viewRef(BQ_DAILY_SUMMARY_VIEW)} AS
    SELECT
      agent_id,
      DATE(start_time) AS shift_date,
      SUM(CASE WHEN activity_type = 'PRODUCTIVE' THEN duration_seconds ELSE 0 END) AS productive_seconds,
      SUM(CASE WHEN activity_type IN ('BREAK','ONE_ON_ONE','HUDDLE','MEETING','NON_PRODUCTIVE')
               THEN duration_seconds ELSE 0 END) AS non_productive_seconds,
      SUM(CASE WHEN activity_type = 'IDLE' THEN duration_seconds ELSE 0 END) AS idle_seconds,
      COUNT(1) AS record_count
    FROM ${tableRef(BQ_ACTIVITY_TABLE)}
    GROUP BY agent_id, DATE(start_time)
  `;
  await bigquery.query({ query: viewQuery, location: BQ_LOCATION });
  logInfo(`[BQ] View ensured: ${getProjectId()}.${BQ_DATASET}.${BQ_DAILY_SUMMARY_VIEW}`);
}

async function runBigQueryHealthCheck() {
  await bigquery.query({
    query: 'SELECT 1 AS ok',
    location: BQ_LOCATION
  });
  logVerbose('[BQ] Health check query succeeded');
}

async function upsertActivityLog(activity) {
  const row = {
    id: normalizeOptionalString(activity.id) || crypto.randomUUID(),
    agent_id: normalizeOptionalString(activity.agent_id),
    activity_type: normalizeOptionalString(activity.activity_type),
    sub_category: normalizeOptionalString(activity.sub_category),
    url: normalizeOptionalString(activity.url),
    page_title: normalizeOptionalString(activity.page_title),
    additional_info: normalizeOptionalString(activity.additional_info),
    start_time: parseTimestamp(activity.start_time) || new Date(),
    end_time: parseTimestamp(activity.end_time),
    duration_seconds: Number.isFinite(Number(activity.duration_seconds)) ? Number(activity.duration_seconds) : 0,
    manually_categorized: Boolean(activity.manually_categorized),
    logged_at: new Date()
  };

  if (!row.agent_id || !row.activity_type) {
    throw new Error('agent_id and activity_type are required');
  }

  const query = `
    MERGE ${tableRef(BQ_ACTIVITY_TABLE)} AS target
    USING (
      SELECT
        @id AS id,
        @agent_id AS agent_id,
        @activity_type AS activity_type,
        @sub_category AS sub_category,
        @url AS url,
        @page_title AS page_title,
        @additional_info AS additional_info,
        @start_time AS start_time,
        @end_time AS end_time,
        @duration_seconds AS duration_seconds,
        @manually_categorized AS manually_categorized,
        @logged_at AS logged_at
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN
      UPDATE SET
        agent_id = source.agent_id,
        activity_type = source.activity_type,
        sub_category = source.sub_category,
        url = source.url,
        page_title = source.page_title,
        additional_info = source.additional_info,
        start_time = source.start_time,
        end_time = source.end_time,
        duration_seconds = source.duration_seconds,
        manually_categorized = source.manually_categorized,
        logged_at = source.logged_at
    WHEN NOT MATCHED THEN
      INSERT (
        id,
        agent_id,
        activity_type,
        sub_category,
        url,
        page_title,
        additional_info,
        start_time,
        end_time,
        duration_seconds,
        manually_categorized,
        logged_at
      )
      VALUES (
        source.id,
        source.agent_id,
        source.activity_type,
        source.sub_category,
        source.url,
        source.page_title,
        source.additional_info,
        source.start_time,
        source.end_time,
        source.duration_seconds,
        source.manually_categorized,
        source.logged_at
      )
  `;

  await bigquery.query({
    query,
    location: BQ_LOCATION,
    params: row,
    types: {
      id: 'STRING',
      agent_id: 'STRING',
      activity_type: 'STRING',
      sub_category: 'STRING',
      url: 'STRING',
      page_title: 'STRING',
      additional_info: 'STRING',
      start_time: 'TIMESTAMP',
      end_time: 'TIMESTAMP',
      duration_seconds: 'INT64',
      manually_categorized: 'BOOL',
      logged_at: 'TIMESTAMP'
    }
  });

  logVerbose(`[BQ] Activity upsert OK id=${row.id} agent=${row.agent_id} type=${row.activity_type}`);
  return row.id;
}

async function upsertAgentStatus(status) {
  const row = {
    agent_id: normalizeOptionalString(status.agent_id),
    current_status: normalizeOptionalString(status.current_status),
    current_activity: normalizeOptionalString(status.current_activity),
    current_url: normalizeOptionalString(status.current_url),
    status_updated_at: parseTimestamp(status.status_updated_at) || new Date(),
    last_seen: parseTimestamp(status.last_seen) || new Date()
  };

  if (!row.agent_id) {
    throw new Error('agent_id is required');
  }

  const query = `
    MERGE ${tableRef(BQ_STATUS_TABLE)} AS target
    USING (
      SELECT
        @agent_id AS agent_id,
        @current_status AS current_status,
        @current_activity AS current_activity,
        @current_url AS current_url,
        @status_updated_at AS status_updated_at,
        @last_seen AS last_seen
    ) AS source
    ON target.agent_id = source.agent_id
    WHEN MATCHED THEN
      UPDATE SET
        current_status = source.current_status,
        current_activity = source.current_activity,
        current_url = source.current_url,
        status_updated_at = source.status_updated_at,
        last_seen = source.last_seen
    WHEN NOT MATCHED THEN
      INSERT (
        agent_id,
        current_status,
        current_activity,
        current_url,
        status_updated_at,
        last_seen
      )
      VALUES (
        source.agent_id,
        source.current_status,
        source.current_activity,
        source.current_url,
        source.status_updated_at,
        source.last_seen
      )
  `;

  await bigquery.query({
    query,
    location: BQ_LOCATION,
    params: row,
    types: {
      agent_id: 'STRING',
      current_status: 'STRING',
      current_activity: 'STRING',
      current_url: 'STRING',
      status_updated_at: 'TIMESTAMP',
      last_seen: 'TIMESTAMP'
    }
  });

  logVerbose(`[BQ] Agent status upsert OK agent=${row.agent_id} status=${row.current_status || 'UNKNOWN'}`);
}

app.get('/health', async (req, res) => {
  try {
    await runBigQueryHealthCheck();
    logVerbose('[API] /health OK');
    res.json({
      status: 'ok',
      timestamp: nowISO(),
      backend: 'bigquery',
      dataset: `${getProjectId()}.${BQ_DATASET}`
    });
  } catch (err) {
    res.status(500).json({ status: 'error', message: err.message });
  }
});

app.post('/api/activity-logs', requireApiKey, async (req, res) => {
  try {
    const id = await upsertActivityLog(req.body || {});
    logInfo(`[API] /api/activity-logs OK id=${id}`);
    res.json({ success: true, id });
  } catch (err) {
    logError('[API] /api/activity-logs error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/agent-status', requireApiKey, async (req, res) => {
  try {
    const agentId = String(req.body?.agent_id || '').trim() || 'UNKNOWN';
    await upsertAgentStatus(req.body || {});
    logInfo(`[API] /api/agent-status OK agent=${agentId}`);
    res.json({ success: true });
  } catch (err) {
    logError('[API] /api/agent-status error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/agent-lookup', requireApiKey, async (req, res) => {
  try {
    const requestedOhr = String(req.body?.ohr_id || req.body?.agent_ohr || req.body?.id || '').trim();

    if (!requestedOhr) {
      return res.status(400).json({ success: false, error: 'ohr_id is required' });
    }

    const csvText = await fetchSheetText(OHR_SHEET_CSV_URL);
    const agents = parseAgentCSV(csvText);

    const match = agents.find((agent) => normalizeOhrId(agent.agent_ohr) === normalizeOhrId(requestedOhr));

    if (!match) {
      return res.status(404).json({
        success: false,
        error: 'OHR ID not found',
        requested_ohr: requestedOhr
      });
    }

    const registrationTimestamp = nowISO();
    const timestampResult = await tryWriteTimestampToWebhook({
      timestamp: registrationTimestamp,
      requested_ohr: requestedOhr,
      agent_ohr: match.agent_ohr,
      agent_name: match.agent_name,
      supervisor: match.supervisor,
      department: match.department
    });

    logInfo(`[API] /api/agent-lookup OK ohr=${requestedOhr}`);
    res.json({
      success: true,
      id: match.id,
      agent_ohr: match.agent_ohr,
      agent_name: match.agent_name,
      supervisor: match.supervisor,
      department: match.department,
      registered_at: registrationTimestamp,
      timestamp_logged: timestampResult.logged,
      timestamp_log_status: timestampResult.status
    });
  } catch (err) {
    logError('[API] /api/agent-lookup error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

function safeWsSend(ws, payload) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
  }
}

let wsClientSequence = 0;

function setupWebSocketServer(server) {
  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws, req) => {
    ws.clientId = ++wsClientSequence;
    ws.isAlive = true;
    ws.isAuthenticated = false;
    const remoteIp = req?.socket?.remoteAddress || 'unknown';
    logInfo(`[WS] Client connected id=${ws.clientId} ip=${remoteIp}`);

    ws.on('pong', () => {
      ws.isAlive = true;
      logVerbose(`[WS] pong id=${ws.clientId}`);
    });

    ws.on('message', async (raw) => {
      let message = null;
      try {
        message = JSON.parse(raw.toString('utf8'));
      } catch (err) {
        logWarn(`[WS] invalid JSON id=${ws.clientId}`);
        safeWsSend(ws, { type: 'error', error: 'invalid_json' });
        return;
      }

      const messageId = message.id || null;
      logVerbose(`[WS] message id=${ws.clientId} type=${message.type || 'unknown'} msgId=${messageId || 'none'}`);

      if (!ws.isAuthenticated) {
        if (message.type !== 'auth') {
          logWarn(`[WS] auth required id=${ws.clientId}`);
          safeWsSend(ws, { type: 'error', id: messageId, error: 'not_authenticated' });
          ws.close(1008, 'Auth required');
          return;
        }

        const receivedKey = message.api_key || message.payload?.api_key;
        if (!receivedKey || receivedKey !== API_KEY) {
          logWarn(`[WS] auth failed id=${ws.clientId}`);
          safeWsSend(ws, { type: 'error', id: messageId, error: 'unauthorized' });
          ws.close(1008, 'Unauthorized');
          return;
        }

        ws.isAuthenticated = true;
        logInfo(`[WS] auth OK id=${ws.clientId}`);
        safeWsSend(ws, { type: 'auth_ok', id: messageId, ts: nowISO() });
        return;
      }

      try {
        if (message.type === 'activity_log') {
          const id = await upsertActivityLog(message.payload || {});
          logVerbose(`[WS] activity_log ingested id=${ws.clientId} activityId=${id}`);
          safeWsSend(ws, { type: 'ack', id: messageId, event: 'activity_log', success: true, activity_id: id });
          return;
        }

        if (message.type === 'agent_status') {
          await upsertAgentStatus(message.payload || {});
          logVerbose(`[WS] agent_status ingested id=${ws.clientId}`);
          safeWsSend(ws, { type: 'ack', id: messageId, event: 'agent_status', success: true });
          return;
        }

        if (message.type === 'ping') {
          logVerbose(`[WS] ping received id=${ws.clientId}`);
          safeWsSend(ws, { type: 'pong', id: messageId, ts: nowISO() });
          return;
        }

        logWarn(`[WS] unknown message type id=${ws.clientId} type=${message.type}`);
        safeWsSend(ws, { type: 'error', id: messageId, error: 'unknown_message_type' });
      } catch (err) {
        logError(`[WS] message processing error id=${ws.clientId}:`, err.message || 'processing_error');
        safeWsSend(ws, { type: 'error', id: messageId, error: err.message || 'processing_error' });
      }
    });

    ws.on('error', (err) => {
      logWarn(`[WS] client error id=${ws.clientId}: ${err.message}`);
    });

    ws.on('close', (code, reasonBuffer) => {
      const reason = reasonBuffer ? reasonBuffer.toString() : '';
      logInfo(`[WS] Client disconnected id=${ws.clientId} code=${code} reason=${reason || 'none'}`);
    });
  });

  const heartbeat = setInterval(() => {
    wss.clients.forEach((client) => {
      if (client.isAlive === false) {
        logWarn(`[WS] heartbeat timeout, terminating id=${client.clientId || 'unknown'}`);
        client.terminate();
        return;
      }
      client.isAlive = false;
      client.ping();
    });
  }, 30000);

  wss.on('close', () => clearInterval(heartbeat));
  logInfo('[WS] WebSocket server initialized at path /ws');
  return wss;
}

async function bootstrap() {
  if (!API_KEY) {
    throw new Error('API_KEY is required');
  }

  await ensureBigQueryObjects();
  await runBigQueryHealthCheck();
  logInfo('[BQ] Startup connectivity check passed');

  const server = http.createServer(app);
  setupWebSocketServer(server);

  server.listen(PORT, () => {
    logInfo(`[BOOT] Time Tracker API running on port ${PORT}`);
    logInfo(`[BOOT] BigQuery dataset: ${getProjectId()}.${BQ_DATASET}`);
    logInfo(`[BOOT] BigQuery auth: ${bigQueryAuthSource}`);
    logInfo('[BOOT] WebSocket endpoint: /ws');
    logInfo(`[BOOT] OHR sheet CSV: ${OHR_SHEET_CSV_URL}`);
    logInfo(`[BOOT] VERBOSE_LOGS=${VERBOSE_LOGS ? 'true' : 'false'}`);
    if (OHR_TIMESTAMP_WEBHOOK_URL) {
      logInfo('[BOOT] OHR timestamp webhook: configured');
    } else {
      logInfo('[BOOT] OHR timestamp webhook: not configured');
    }
    if (ALLOW_INSECURE_TLS) {
      logWarn('WARNING: insecure TLS mode enabled (ALLOW_INSECURE_TLS=true)');
    }
  });
}

bootstrap().catch((err) => {
  logError('[BOOT] Startup error:', err.message);
  process.exit(1);
});
