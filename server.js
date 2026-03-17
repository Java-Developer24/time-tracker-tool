require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const https = require('https');
const path = require('path');
const crypto = require('crypto');
const { BigQuery } = require('@google-cloud/bigquery');
const { WebSocketServer } = require('ws');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

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

function parseBoundedInt(value, fallback, min, max) {
  const parsed = parseInt(String(value || ''), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, min), max);
}

function getDashboardDateParam(value) {
  const raw = String(value || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  return new Date().toISOString().slice(0, 10);
}

function getSinceTimestamp(minutes) {
  return new Date(Date.now() - (minutes * 60 * 1000));
}

function normalizeDashboardStatus(value) {
  const status = normalizeOptionalString(value);
  if (!status) return null;

  const upper = status.toUpperCase();
  if (upper === 'PRODUCTIVE' || upper === 'NON_PRODUCTIVE' || upper === 'IDLE') {
    return upper;
  }
  return null;
}

function toSafeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toIsoOrNull(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
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

async function runDashboardQuery(query, params, types) {
  const [rows] = await bigquery.query({
    query,
    location: BQ_LOCATION,
    params: params || {},
    types: types || {}
  });
  return rows;
}

function serializeDashboardAgent(row) {
  return {
    agent_id: row.agent_id,
    current_status: row.current_status || 'UNKNOWN',
    current_activity: row.current_activity || '',
    current_url: row.current_url || '',
    status_updated_at: toIsoOrNull(row.status_updated_at),
    last_seen: toIsoOrNull(row.last_seen),
    productive_seconds_today: toSafeNumber(row.productive_seconds_today),
    non_productive_seconds_today: toSafeNumber(row.non_productive_seconds_today),
    idle_seconds_today: toSafeNumber(row.idle_seconds_today),
    activity_count_today: toSafeNumber(row.activity_count_today)
  };
}

function serializeDashboardActivity(row) {
  return {
    id: row.id,
    agent_id: row.agent_id,
    activity_type: row.activity_type,
    sub_category: row.sub_category || '',
    url: row.url || '',
    page_title: row.page_title || '',
    start_time: toIsoOrNull(row.start_time),
    end_time: toIsoOrNull(row.end_time),
    duration_seconds: toSafeNumber(row.duration_seconds),
    manually_categorized: !!row.manually_categorized,
    logged_at: toIsoOrNull(row.logged_at)
  };
}

function getDashboardSummaryFilters(input = {}) {
  const shiftDate = getDashboardDateParam(input.date || input.shift_date);
  const windowMinutes = parseBoundedInt(input.window_minutes, 60, 5, 1440);
  return {
    shift_date: shiftDate,
    window_minutes: windowMinutes,
    since_ts: getSinceTimestamp(windowMinutes)
  };
}

function getDashboardAgentsFilters(input = {}) {
  return {
    shift_date: getDashboardDateParam(input.date || input.shift_date),
    status: normalizeDashboardStatus(input.status),
    search: normalizeOptionalString(input.search),
    limit: parseBoundedInt(input.limit, 100, 1, 500)
  };
}

function getDashboardActivityFeedFilters(input = {}) {
  const limit = parseBoundedInt(input.limit, 50, 1, 200);
  const windowMinutes = parseBoundedInt(input.window_minutes, 180, 5, 10080);
  const activityType = normalizeOptionalString(input.activity_type);

  return {
    agent_id: normalizeOptionalString(input.agent_id),
    activity_type: activityType ? activityType.toUpperCase() : null,
    window_minutes: windowMinutes,
    limit,
    since_ts: getSinceTimestamp(windowMinutes)
  };
}

function getDashboardSubscriptionFilters(input = {}) {
  const windowMinutes = parseBoundedInt(input.window_minutes, 60, 5, 1440);
  return {
    shift_date: getDashboardDateParam(input.date || input.shift_date),
    window_minutes: windowMinutes,
    status: normalizeDashboardStatus(input.status),
    search: normalizeOptionalString(input.search),
    limit: parseBoundedInt(input.limit, 100, 1, 500),
    activity_limit: parseBoundedInt(input.activity_limit, 25, 1, 200),
    activity_window_minutes: parseBoundedInt(
      input.activity_window_minutes,
      Math.max(windowMinutes, 180),
      5,
      10080
    )
  };
}

async function getDashboardSummaryData(input = {}) {
  const filters = getDashboardSummaryFilters(input);

  const summaryQuery = `
    WITH current_status AS (
      SELECT
        COUNT(*) AS total_agents,
        COUNTIF(UPPER(COALESCE(current_status, '')) = 'PRODUCTIVE') AS productive_agents,
        COUNTIF(UPPER(COALESCE(current_status, '')) = 'NON_PRODUCTIVE') AS non_productive_agents,
        COUNTIF(UPPER(COALESCE(current_status, '')) = 'IDLE') AS idle_agents,
        COUNTIF(last_seen >= @since_ts) AS agents_seen_in_window,
        MAX(last_seen) AS latest_agent_seen_at
      FROM ${tableRef(BQ_STATUS_TABLE)}
    ),
    day_activity AS (
      SELECT
        COUNT(*) AS activity_count_today,
        COALESCE(SUM(duration_seconds), 0) AS total_duration_seconds_today,
        COALESCE(SUM(IF(activity_type = 'PRODUCTIVE', duration_seconds, 0)), 0) AS productive_seconds_today,
        COALESCE(SUM(IF(activity_type = 'NON_PRODUCTIVE', duration_seconds, 0)), 0) AS non_productive_seconds_today,
        COALESCE(SUM(IF(activity_type = 'IDLE', duration_seconds, 0)), 0) AS idle_seconds_today,
        COALESCE(MAX(logged_at), MAX(start_time)) AS latest_activity_at
      FROM ${tableRef(BQ_ACTIVITY_TABLE)}
      WHERE DATE(start_time) = @shift_date
    ),
    recent_activity AS (
      SELECT
        COUNT(*) AS activity_count_in_window
      FROM ${tableRef(BQ_ACTIVITY_TABLE)}
      WHERE logged_at >= @since_ts
    )
    SELECT *
    FROM current_status
    CROSS JOIN day_activity
    CROSS JOIN recent_activity
  `;

  const topCategoriesQuery = `
    SELECT
      COALESCE(sub_category, 'UNSPECIFIED') AS sub_category,
      COUNT(*) AS event_count,
      COALESCE(SUM(duration_seconds), 0) AS total_duration_seconds
    FROM ${tableRef(BQ_ACTIVITY_TABLE)}
    WHERE DATE(start_time) = @shift_date
    GROUP BY sub_category
    ORDER BY total_duration_seconds DESC, event_count DESC
    LIMIT 6
  `;

  const [summaryRows, topCategories] = await Promise.all([
    runDashboardQuery(summaryQuery, {
      shift_date: filters.shift_date,
      since_ts: filters.since_ts
    }, {
      shift_date: 'DATE',
      since_ts: 'TIMESTAMP'
    }),
    runDashboardQuery(topCategoriesQuery, {
      shift_date: filters.shift_date
    }, {
      shift_date: 'DATE'
    })
  ]);

  const row = summaryRows[0] || {};
  return {
    filters: {
      shift_date: filters.shift_date,
      window_minutes: filters.window_minutes
    },
    summary: {
      total_agents: toSafeNumber(row.total_agents),
      productive_agents: toSafeNumber(row.productive_agents),
      non_productive_agents: toSafeNumber(row.non_productive_agents),
      idle_agents: toSafeNumber(row.idle_agents),
      agents_seen_in_window: toSafeNumber(row.agents_seen_in_window),
      activity_count_today: toSafeNumber(row.activity_count_today),
      activity_count_in_window: toSafeNumber(row.activity_count_in_window),
      total_duration_seconds_today: toSafeNumber(row.total_duration_seconds_today),
      productive_seconds_today: toSafeNumber(row.productive_seconds_today),
      non_productive_seconds_today: toSafeNumber(row.non_productive_seconds_today),
      idle_seconds_today: toSafeNumber(row.idle_seconds_today),
      latest_agent_seen_at: toIsoOrNull(row.latest_agent_seen_at),
      latest_activity_at: toIsoOrNull(row.latest_activity_at)
    },
    top_subcategories: topCategories.map((item) => ({
      sub_category: item.sub_category || 'UNSPECIFIED',
      event_count: toSafeNumber(item.event_count),
      total_duration_seconds: toSafeNumber(item.total_duration_seconds)
    }))
  };
}

async function getDashboardAgentsData(input = {}) {
  const filters = getDashboardAgentsFilters(input);

  const query = `
    SELECT
      s.agent_id,
      COALESCE(s.current_status, 'UNKNOWN') AS current_status,
      COALESCE(s.current_activity, '') AS current_activity,
      COALESCE(s.current_url, '') AS current_url,
      s.status_updated_at,
      s.last_seen,
      COALESCE(d.productive_seconds, 0) AS productive_seconds_today,
      COALESCE(d.non_productive_seconds, 0) AS non_productive_seconds_today,
      COALESCE(d.idle_seconds, 0) AS idle_seconds_today,
      COALESCE(d.record_count, 0) AS activity_count_today
    FROM ${tableRef(BQ_STATUS_TABLE)} AS s
    LEFT JOIN ${viewRef(BQ_DAILY_SUMMARY_VIEW)} AS d
      ON d.agent_id = s.agent_id
     AND d.shift_date = @shift_date
    WHERE (@status_filter IS NULL OR UPPER(s.current_status) = @status_filter)
      AND (@search_term IS NULL OR LOWER(s.agent_id) LIKE CONCAT('%', @search_term, '%'))
    ORDER BY s.last_seen DESC
    LIMIT @limit
  `;

  const rows = await runDashboardQuery(query, {
    shift_date: filters.shift_date,
    status_filter: filters.status,
    search_term: filters.search ? filters.search.toLowerCase() : null,
    limit: filters.limit
  }, {
    shift_date: 'DATE',
    status_filter: 'STRING',
    search_term: 'STRING',
    limit: 'INT64'
  });

  return {
    filters: {
      shift_date: filters.shift_date,
      status: filters.status,
      search: filters.search || null,
      limit: filters.limit
    },
    agents: rows.map(serializeDashboardAgent)
  };
}

async function getDashboardActivityFeedData(input = {}) {
  const filters = getDashboardActivityFeedFilters(input);

  const query = `
    SELECT
      id,
      agent_id,
      activity_type,
      COALESCE(sub_category, '') AS sub_category,
      COALESCE(url, '') AS url,
      COALESCE(page_title, '') AS page_title,
      start_time,
      end_time,
      COALESCE(duration_seconds, 0) AS duration_seconds,
      manually_categorized,
      logged_at
    FROM ${tableRef(BQ_ACTIVITY_TABLE)}
    WHERE logged_at >= @since_ts
      AND (@agent_id IS NULL OR agent_id = @agent_id)
      AND (@activity_type IS NULL OR UPPER(activity_type) = @activity_type)
    ORDER BY logged_at DESC
    LIMIT @limit
  `;

  const rows = await runDashboardQuery(query, {
    since_ts: filters.since_ts,
    agent_id: filters.agent_id,
    activity_type: filters.activity_type,
    limit: filters.limit
  }, {
    since_ts: 'TIMESTAMP',
    agent_id: 'STRING',
    activity_type: 'STRING',
    limit: 'INT64'
  });

  return {
    filters: {
      agent_id: filters.agent_id,
      activity_type: filters.activity_type,
      window_minutes: filters.window_minutes,
      limit: filters.limit
    },
    activities: rows.map(serializeDashboardActivity)
  };
}

async function getDashboardSnapshotData(input = {}) {
  const subscription = getDashboardSubscriptionFilters(input);
  const [summaryData, agentsData, activityFeedData] = await Promise.all([
    getDashboardSummaryData({
      shift_date: subscription.shift_date,
      window_minutes: subscription.window_minutes
    }),
    getDashboardAgentsData({
      shift_date: subscription.shift_date,
      status: subscription.status,
      search: subscription.search,
      limit: subscription.limit
    }),
    getDashboardActivityFeedData({
      window_minutes: subscription.activity_window_minutes,
      limit: subscription.activity_limit
    })
  ]);

  return {
    filters: {
      shift_date: subscription.shift_date,
      window_minutes: subscription.window_minutes,
      status: subscription.status,
      search: subscription.search || null,
      limit: subscription.limit,
      activity_limit: subscription.activity_limit,
      activity_window_minutes: subscription.activity_window_minutes
    },
    summary: summaryData.summary,
    top_subcategories: summaryData.top_subcategories,
    agents: agentsData.agents,
    activities: activityFeedData.activities
  };
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
  return row;
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
  return row;
}

app.get('/test', (req, res) => {
  logVerbose('[API] /test OK');
  res.json({
    status: 'ok',
    message: 'Backend is running',
    timestamp: nowISO(),
    uptime_seconds: Math.floor(process.uptime())
  });
});

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

app.get('/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard-sample.html'));
});

app.get('/api/dashboard/summary', requireApiKey, async (req, res) => {
  try {
    const data = await getDashboardSummaryData(req.query || {});
    res.json({
      success: true,
      generated_at: nowISO(),
      ...data
    });
  } catch (err) {
    logError('[API] /api/dashboard/summary error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/dashboard/agents', requireApiKey, async (req, res) => {
  try {
    const data = await getDashboardAgentsData(req.query || {});
    res.json({
      success: true,
      generated_at: nowISO(),
      ...data
    });
  } catch (err) {
    logError('[API] /api/dashboard/agents error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/dashboard/activity-feed', requireApiKey, async (req, res) => {
  try {
    const data = await getDashboardActivityFeedData(req.query || {});
    res.json({
      success: true,
      generated_at: nowISO(),
      ...data
    });
  } catch (err) {
    logError('[API] /api/dashboard/activity-feed error:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/activity-logs', requireApiKey, async (req, res) => {
  try {
    const row = await upsertActivityLog(req.body || {});
    broadcastDashboardSnapshots('activity_log');
    logInfo(`[API] /api/activity-logs OK id=${row.id}`);
    res.json({ success: true, id: row.id });
  } catch (err) {
    logError('[API] /api/activity-logs error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/agent-status', requireApiKey, async (req, res) => {
  try {
    const agentId = String(req.body?.agent_id || '').trim() || 'UNKNOWN';
    await upsertAgentStatus(req.body || {});
    broadcastDashboardSnapshots('agent_status');
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

let dashboardWss = null;

async function sendDashboardSnapshot(ws, reason, messageId) {
  if (!ws || ws.readyState !== 1 || !ws.isAuthenticated || !ws.dashboardSubscription) {
    return;
  }

  try {
    const snapshot = await getDashboardSnapshotData(ws.dashboardSubscription);
    safeWsSend(ws, {
      type: 'dashboard_snapshot',
      id: messageId || null,
      reason,
      generated_at: nowISO(),
      payload: snapshot
    });
  } catch (err) {
    logError(`[WS] dashboard snapshot error id=${ws.clientId}:`, err.message);
    safeWsSend(ws, {
      type: 'dashboard_error',
      id: messageId || null,
      error: err.message || 'dashboard_snapshot_failed'
    });
  }
}

function scheduleDashboardSnapshot(ws, reason) {
  if (!ws || !ws.dashboardSubscription) {
    return;
  }

  if (ws.dashboardSnapshotTimer) {
    clearTimeout(ws.dashboardSnapshotTimer);
  }

  ws.dashboardSnapshotTimer = setTimeout(() => {
    ws.dashboardSnapshotTimer = null;
    sendDashboardSnapshot(ws, reason).catch((err) => {
      logError(`[WS] dashboard push error id=${ws.clientId}:`, err.message);
    });
  }, 300);
}

function broadcastDashboardSnapshots(reason) {
  if (!dashboardWss) {
    return;
  }

  dashboardWss.clients.forEach((client) => {
    if (!client.isAuthenticated || !client.dashboardSubscription) {
      return;
    }
    scheduleDashboardSnapshot(client, reason);
  });
}

let wsClientSequence = 0;

function setupWebSocketServer(server) {
  const wss = new WebSocketServer({ server, path: '/ws' });
  dashboardWss = wss;

  wss.on('connection', (ws, req) => {
    ws.clientId = ++wsClientSequence;
    ws.isAlive = true;
    ws.isAuthenticated = false;
    ws.dashboardSubscription = null;
    ws.dashboardSnapshotTimer = null;
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
          const row = await upsertActivityLog(message.payload || {});
          broadcastDashboardSnapshots('activity_log');
          logVerbose(`[WS] activity_log ingested id=${ws.clientId} activityId=${row.id}`);
          safeWsSend(ws, { type: 'ack', id: messageId, event: 'activity_log', success: true, activity_id: row.id });
          return;
        }

        if (message.type === 'agent_status') {
          await upsertAgentStatus(message.payload || {});
          broadcastDashboardSnapshots('agent_status');
          logVerbose(`[WS] agent_status ingested id=${ws.clientId}`);
          safeWsSend(ws, { type: 'ack', id: messageId, event: 'agent_status', success: true });
          return;
        }

        if (message.type === 'subscribe_dashboard') {
          ws.dashboardSubscription = getDashboardSubscriptionFilters(message.payload || {});
          logInfo(`[WS] dashboard subscribed id=${ws.clientId}`);
          await sendDashboardSnapshot(ws, 'subscribed', messageId);
          return;
        }

        if (message.type === 'unsubscribe_dashboard') {
          if (ws.dashboardSnapshotTimer) {
            clearTimeout(ws.dashboardSnapshotTimer);
            ws.dashboardSnapshotTimer = null;
          }
          ws.dashboardSubscription = null;
          logInfo(`[WS] dashboard unsubscribed id=${ws.clientId}`);
          safeWsSend(ws, { type: 'dashboard_unsubscribed', id: messageId, ts: nowISO() });
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
      if (ws.dashboardSnapshotTimer) {
        clearTimeout(ws.dashboardSnapshotTimer);
      }
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
