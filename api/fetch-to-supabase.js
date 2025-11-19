// api/fetch-to-supabase.js
// Vercel serverless (CommonJS)
// Batch import per X locations (default 10), delta-sync, dynamic columns
// Uses Postgres connection to Supabase (service role / admin connection string required)

const fetch = require('node-fetch');
const { Client } = require('pg');

const AUTH_DOMAIN = process.env.AUTH_DOMAIN;           // blik.eu.auth0.com
const API_DOMAIN = process.env.API_DOMAIN;           // water-backend.blik.cloud
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;
const PG_SUPABASE_URL = process.env.PG_SUPABASE_URL; // postgres://<service_role>@... (required for ALTER TABLE)
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'BLIK_api';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10', 10);
const LIMIT_PER_PAGE = parseInt(process.env.LIMIT_PER_PAGE || '1000', 10);

// Path to your uploaded metadata (for reference / debugging)
// (Developer note: user uploaded file path)
const METADATA_JSON_PATH = '/mnt/data/blik-metadata-v3.json';

// ---- helpers ----
async function getAccessToken() {
  const tokenUrl = `https://${AUTH_DOMAIN}/oauth/token`;
  const body = {
    grant_type: 'client_credentials',
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    audience: `https://${API_DOMAIN}/api`
  };

  const res = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Auth token request failed: ${res.status} ${t}`);
  }
  const data = await res.json();
  if (!data.access_token) throw new Error('No access token returned');
  return data.access_token;
}

function sanitizeCol(name) {
  return name.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase().slice(0, 63);
}

function guessSqlType(value) {
  if (value === null) return 'text';
  if (Array.isArray(value) || typeof value === 'object') return 'jsonb';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'number') {
    if (Number.isInteger(value)) return 'bigint';
    return 'double precision';
  }
  if (typeof value === 'string') {
    const isoLike = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
    if (isoLike.test(value)) return 'timestamptz';
    return 'text';
  }
  return 'text';
}

async function ensureTable(client) {
  const sql = `
    CREATE TABLE IF NOT EXISTS "${SUPABASE_TABLE}" (
      id SERIAL PRIMARY KEY,
      measurement_id BIGINT,
      location_id BIGINT,
      timestamp timestamptz,
      data jsonb,
      inserted_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    );
    `;
  await client.query(sql);

  // create unique index on location_id + timestamp to allow upsert/conflict handling
  const idxSql = `CREATE UNIQUE INDEX IF NOT EXISTS ${SUPABASE_TABLE}_uniq_location_time ON "${SUPABASE_TABLE}" (location_id, timestamp);`;
  await client.query(idxSql);
}

async function getExistingColumns(client) {
  const res = await client.query(
    `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`,
    [SUPABASE_TABLE.toLowerCase()]
  );
  const map = {};
  for (const r of res.rows) map[r.column_name] = r.data_type;
  return map;
}

async function addColumn(client, colName, sqlType) {
  const safe = sanitizeCol(colName);
  const sql = `ALTER TABLE "${SUPABASE_TABLE}" ADD COLUMN IF NOT EXISTS "${safe}" ${sqlType};`;
  await client.query(sql);
}

async function upsertMeasurement(client, meas, existingCols) {
  // top-level keys
  const top = { ...meas };

  // unify certain fields
  const measurement_id_val = top.id ?? null;
  const location_id_val = top.locationId ?? top.location_id ?? top.location ?? null;
  const timestamp_val = top.timestamp ?? top.time ?? null;

  // prepare dynamic columns from top-level keys except reserved
  const reserved = new Set(['id','measurement_id','locationId','location_id','location','timestamp','time']);
  const dynCols = [];
  const dynValues = [];
  for (const [k,v] of Object.entries(top)) {
    if (reserved.has(k)) continue;
    const col = sanitizeCol(k);
    if (!existingCols[col]) {
      const t = guessSqlType(v);
      await addColumn(client, col, t);
      existingCols[col] = true;
    }
    dynCols.push(col);
    dynValues.push(v);
  }

  // Build insert
  const baseCols = ['measurement_id','location_id','timestamp','data'];
  const allCols = baseCols.concat(dynCols);
  const params = [];
  // measurement_id
  params.push(measurement_id_val);
  // location_id
  params.push(location_id_val);
  // timestamp
  params.push(timestamp_val);
  // data
  params.push(meas);

  // push dyn values
  for (const v of dynValues) params.push(v);

  // placeholders
  const placeholders = allCols.map((_, i) => `$${i+1}`).join(', ');

  // quoted column names
  const quotedCols = allCols.map(c => `"${c}"`).join(', ');

  // Build update list for ON CONFLICT
  const updateCols = [`data = EXCLUDED.data`, `updated_at = now()`].concat(dynCols.map(c => `"${c}" = EXCLUDED."${c}"`));

  const sql = `INSERT INTO "${SUPABASE_TABLE}" (${quotedCols})
    VALUES (${placeholders})
    ON CONFLICT (location_id, timestamp)
    DO UPDATE SET ${updateCols.join(', ')};`;

  await client.query(sql, params);
}

async function fetchMeasurementsForLocation(token, locationId, sinceTimestamp=null) {
  const results = [];
  let after = null;
  let done = false;

  // If API supports "after" timestamp param for delta, pass sinceTimestamp as parameter if provided.
  // We'll use pagination by 'limit' and 'after' where 'after' equals last timestamp from previous page.
  while (!done) {
    const qs = new URLSearchParams();
    qs.set('limit', String(LIMIT_PER_PAGE));
    if (sinceTimestamp && !after) {
      // Some APIs accept 'after' as ISO timestamp; if not supported, we'll fetch everything and filter locally
      qs.set('after', sinceTimestamp);
    }
    if (after) qs.set('after', after);

    const url = `https://${API_DOMAIN}/api/v2/locations/${locationId}/measurements?${qs.toString()}`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }});
    if (!r.ok) {
      const t = await r.text();
      throw new Error(`Fetch measurements failed for ${locationId}: ${r.status} ${t}`);
    }
    const page = await r.json();
    if (!Array.isArray(page) || page.length === 0) { done = true; break; }
    // If the API doesn't support 'after' filter correctly, we filter out <= sinceTimestamp
    let pageFiltered = page;
    if (sinceTimestamp) {
      pageFiltered = page.filter(p => {
        if (!p.timestamp) return true;
        return new Date(p.timestamp) > new Date(sinceTimestamp);
      });
    }
    results.push(...pageFiltered);

    if (page.length < LIMIT_PER_PAGE) done = true;
    else {
      const last = page[page.length - 1];
      if (last && last.timestamp) after = last.timestamp;
      else done = true;
    }
  }

  return results;
}

async function fetchAllLocations(token) {
  const url = `https://${API_DOMAIN}/api/v3/locations`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }});
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Fetch locations failed: ${r.status} ${t}`);
  }
  const body = await r.json();
  // accept array or object
  if (Array.isArray(body)) return body;
  return Object.values(body);
}

async function getLatestTimestampForLocation(client, locationId) {
  const res = await client.query(
    `SELECT timestamp FROM "${SUPABASE_TABLE}" WHERE location_id = $1 ORDER BY timestamp DESC LIMIT 1`,
    [locationId]
  );
  if (res.rows.length === 0) return null;
  return res.rows[0].timestamp;
}

// ---- handler ----
module.exports = async (req, res) => {
  try {
    // offset param: which batch of locations to start with (0-based)
    const q = req.query || {};
    const offset = parseInt(q.offset || '0', 10);
    const batchSize = parseInt(q.batch_size || String(BATCH_SIZE), 10);

    if (!AUTH_DOMAIN || !API_DOMAIN || !CLIENT_ID || typeof CLIENT_SECRET === 'undefined' || !PG_SUPABASE_URL) {
      return res.status(500).json({ success: false, message: 'Missing required environment variables. Check AUTH_DOMAIN, API_DOMAIN, CLIENT_ID, CLIENT_SECRET, PG_SUPABASE_URL.'});
    }

    const token = await getAccessToken();
    const locations = await fetchAllLocations(token);
    if (!Array.isArray(locations) || locations.length === 0) {
      return res.status(200).json({ success: true, message: 'Geen locaties gevonden', locationsFound: 0 });
    }

    const start = offset;
    const end = Math.min(offset + batchSize, locations.length);
    const batch = locations.slice(start, end);

    const pg = new Client({ connectionString: PG_SUPABASE_URL });
    await pg.connect();
    await ensureTable(pg);
    const existingCols = await getExistingColumns(pg);

    let processedLocations = 0;
    let totalInserted = 0;

    for (const loc of batch) {
      const locationId = loc.id ?? loc.locationId ?? loc.location_id ?? null;
      if (!locationId) continue;
      processedLocations++;

      // get latest timestamp in DB for delta sync
      const latestTs = await getLatestTimestampForLocation(pg, locationId);
      const measurements = await fetchMeasurementsForLocation(token, locationId, latestTs);

      for (const m of measurements) {
        try {
          await upsertMeasurement(pg, m, existingCols);
          totalInserted++;
        } catch (err) {
          console.error('Upsert error', err.message);
        }
      }
    }

    await pg.end();

    // Decide next offset (for automatic run)
    const nextOffset = end < locations.length ? end : null;
    const nextUrl = nextOffset !== null ? `${req.protocol || 'https'}://${req.headers.host}${req.path}?offset=${nextOffset}` : null;

    res.status(200).json({
      success: true,
      batchStart: start,
      batchEnd: end - 1,
      processedLocations,
      totalInserted,
      nextOffset,
      nextUrl,
      metadata_file_path: METADATA_JSON_PATH
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
};
