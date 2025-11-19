// api/fetch-to-supabase.js
// Batch import (10 locations per run) + delta sync + dynamic columns
// Uses Supabase Postgres via service_role connection string (PG_SUPABASE_URL)

const fetch = require('node-fetch');
const { Client } = require('pg');

const AUTH_DOMAIN = process.env.AUTH_DOMAIN;           
const API_DOMAIN = process.env.API_DOMAIN;             
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;

// Important! Must be the service_role connection string
const PG_SUPABASE_URL = process.env.PG_SUPABASE_URL;

const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'BLIK_api';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10', 10);
const LIMIT_PER_PAGE = parseInt(process.env.LIMIT_PER_PAGE || '1000', 10);

const METADATA_PATH = "/mnt/data/blik-metadata-v3.json";   // for reference only

// ------------------------------------------------------------
// AUTH0 TOKEN
// ------------------------------------------------------------
async function getAccessToken() {
  const url = `https://${AUTH_DOMAIN}/oauth/token`;

  const body = {
    grant_type: "client_credentials",
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    audience: "https://water.bliksensing.nl" // ❗ Correct audience
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Auth0 token request failed: ${res.status} ${text}`);
  }

  const json = await res.json();
  if (!json.access_token) throw new Error("No access_token received from Auth0");

  return json.access_token;
}

// ------------------------------------------------------------
// HELPERS
// ------------------------------------------------------------
function sanitizeCol(name) {
  return name.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase().slice(0, 63);
}

function guessSqlType(v) {
  if (v === null) return "text";
  if (Array.isArray(v) || typeof v === "object") return "jsonb";
  if (typeof v === "boolean") return "boolean";
  if (typeof v === "number") return Number.isInteger(v) ? "bigint" : "double precision";
  if (typeof v === "string") {
    const iso = /^\d{4}-\d{2}-\d{2}T/;
    return iso.test(v) ? "timestamptz" : "text";
  }
  return "text";
}

// ------------------------------------------------------------
// CREATE TABLE IF NEEDED
// ------------------------------------------------------------
async function ensureTable(pg) {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS "${SUPABASE_TABLE}" (
      id SERIAL PRIMARY KEY,
      measurement_id BIGINT,
      location_id BIGINT,
      timestamp timestamptz,
      data jsonb,
      inserted_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    );
  `);

  await pg.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ${SUPABASE_TABLE}_uniq_loc_time
      ON "${SUPABASE_TABLE}" (location_id, timestamp);
  `);
}

async function getExistingColumns(pg) {
  const r = await pg.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = $1
  `, [SUPABASE_TABLE.toLowerCase()]);

  const map = {};
  r.rows.forEach(col => map[col.column_name] = true);
  return map;
}

async function addColumn(pg, name, sqlType) {
  const safe = sanitizeCol(name);
  await pg.query(`
    ALTER TABLE "${SUPABASE_TABLE}" 
    ADD COLUMN IF NOT EXISTS "${safe}" ${sqlType};
  `);
}

// ------------------------------------------------------------
// UPSERT MEASUREMENT
// ------------------------------------------------------------
async function upsertMeasurement(pg, meas, existing) {
  const top = { ...meas };

  const measurement_id = top.id ?? null;
  const location_id =
    top.locationId ??
    top.location_id ??
    top.location ??
    null;

  const timestamp =
    top.timestamp ??
    top.time ??
    null;

  const reserved = new Set([
    "id","measurement_id","locationId","location_id","location","timestamp","time"
  ]);

  const dynCols = [];
  const dynVals = [];

  for (const [k, v] of Object.entries(top)) {
    if (reserved.has(k)) continue;

    const col = sanitizeCol(k);
    if (!existing[col]) {
      const type = guessSqlType(v);
      await addColumn(pg, col, type);
      existing[col] = true;
    }

    dynCols.push(col);
    dynVals.push(v);
  }

  const baseCols = ["measurement_id", "location_id", "timestamp", "data"];
  const allCols = baseCols.concat(dynCols);

  const params = [measurement_id, location_id, timestamp, meas, ...dynVals];
  const placeholders = params.map((_, i) => `$${i+1}`).join(", ");
  const colSql = allCols.map(c => `"${c}"`).join(", ");

  const updates = ["data = EXCLUDED.data", "updated_at = now()"]
    .concat(dynCols.map(c => `"${c}" = EXCLUDED."${c}"`));

  await pg.query(`
    INSERT INTO "${SUPABASE_TABLE}" (${colSql})
    VALUES (${placeholders})
    ON CONFLICT (location_id, timestamp)
    DO UPDATE SET ${updates.join(", ")};
  `, params);
}

// ------------------------------------------------------------
// FETCH LOCATIONS
// ------------------------------------------------------------
async function fetchLocations(token) {
  const url = `https://${API_DOMAIN}/api/v3/locations`;
  const r = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` }
  });

  if (!r.ok) {
    const t = await r.text();
    throw new Error(`Failed to load locations: ${r.status} ${t}`);
  }

  const body = await r.json();
  return Array.isArray(body) ? body : Object.values(body);
}

// ------------------------------------------------------------
// LOAD LAST TIMESTAMP FOR DELTA SYNC
// ------------------------------------------------------------
async function getLatestTimestamp(pg, locationId) {
  const r = await pg.query(`
    SELECT timestamp
    FROM "${SUPABASE_TABLE}"
    WHERE location_id = $1
    ORDER BY timestamp DESC
    LIMIT 1
  `, [locationId]);

  return r.rows.length ? r.rows[0].timestamp : null;
}

// ------------------------------------------------------------
// FETCH MEASUREMENTS
// ------------------------------------------------------------
async function fetchMeasurements(token, locationId, since = null) {
  const result = [];
  let after = since || null;
  let done = false;

  while (!done) {
    const qs = new URLSearchParams();
    qs.set("limit", LIMIT_PER_PAGE);
    if (after) qs.set("after", after);

    const url = `https://${API_DOMAIN}/api/v2/locations/${locationId}/measurements?${qs}`;
    const r = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` }
    });

    if (!r.ok) {
      const t = await r.text();
      throw new Error(`Measurements failed (${locationId}): ${r.status} ${t}`);
    }

    const page = await r.json();
    if (!Array.isArray(page) || page.length === 0) break;

    result.push(...page);
    if (page.length < LIMIT_PER_PAGE) break;

    const last = page[page.length - 1];
    if (!last.timestamp) break;
    after = last.timestamp;
  }

  if (since) {
    return result.filter(m => new Date(m.timestamp) > new Date(since));
  }

  return result;
}

// ------------------------------------------------------------
// HANDLER
// ------------------------------------------------------------
module.exports = async (req, res) => {
  try {
    const offset = parseInt(req.query.offset || "0", 10);

    const token = await getAccessToken();
    const locations = await fetchLocations(token);

    const start = offset;
    const end = Math.min(offset + BATCH_SIZE, locations.length);
    const slice = locations.slice(start, end);

    const pg = new Client({ connectionString: PG_SUPABASE_URL });
    await pg.connect();
    await ensureTable(pg);
    const existing = await getExistingColumns(pg);

    let imported = 0;

    for (const loc of slice) {
      const locationId = loc.id ?? loc.locationId ?? loc.location_id;
      if (!locationId) continue;

      const lastTs = await getLatestTimestamp(pg, locationId);
      const measurements = await fetchMeasurements(token, locationId, lastTs);

      for (const m of measurements) {
        await upsertMeasurement(pg, m, existing);
        imported++;
      }
    }

    await pg.end();

    const nextOffset = end < locations.length ? end : null;
    const nextUrl = nextOffset !== null ?
      `${req.url.split("?")[0]}?offset=${nextOffset}` :
      null;

    res.status(200).json({
      success: true,
      batch: { start, end: end - 1 },
      imported,
      nextOffset,
      nextUrl,
      metadata_file: METADATA_PATH
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
};
