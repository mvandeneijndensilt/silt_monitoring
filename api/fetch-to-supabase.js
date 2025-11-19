// api/fetch-to-supabase.js
// Vercel serverless, pooler-compatible, fixed columns (BLIK_api)

const fetch = require('node-fetch');
const { Pool } = require('pg');

const AUTH_DOMAIN = process.env.AUTH_DOMAIN;           
const API_DOMAIN = process.env.API_DOMAIN;             
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;

const PG_SUPABASE_URL = process.env.PG_SUPABASE_URL;

const SUPABASE_TABLE = process.env.SUPABASE_TABLE || 'BLIK_api';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '10', 10);
const LIMIT_PER_PAGE = parseInt(process.env.LIMIT_PER_PAGE || '1000', 10);

const METADATA_PATH = "/mnt/data/blik-metadata-v3.json";

// ------------------------------------------------------------
// AUTH0 TOKEN
// ------------------------------------------------------------
async function getAccessToken() {
  const res = await fetch(`https://${AUTH_DOMAIN}/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      grant_type: "client_credentials",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      audience: "https://water.bliksensing.nl"
    })
  });

  if (!res.ok) throw new Error(`Auth0 token request failed: ${res.status} ${await res.text()}`);
  const json = await res.json();
  if (!json.access_token) throw new Error("No access_token received from Auth0");
  return json.access_token;
}

// ------------------------------------------------------------
// FETCH LOCATIONS
// ------------------------------------------------------------
async function fetchLocations(token) {
  const res = await fetch(`https://${API_DOMAIN}/api/v3/locations`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) throw new Error(`Failed to load locations: ${res.status} ${await res.text()}`);
  const body = await res.json();
  return Array.isArray(body) ? body : Object.values(body);
}

// ------------------------------------------------------------
// FETCH MEASUREMENTS (delta sync)
// ------------------------------------------------------------
async function fetchMeasurements(token, locationId, since = null) {
  const result = [];
  let after = since || null;

  while (true) {
    const qs = new URLSearchParams({ limit: LIMIT_PER_PAGE });
    if (after) qs.set("after", after);

    const res = await fetch(`https://${API_DOMAIN}/api/v2/locations/${locationId}/measurements?${qs}`, {
      headers: { Authorization: `Bearer ${token}` }
    });

    if (!res.ok) throw new Error(`Measurements failed (${locationId}): ${res.status} ${await res.text()}`);
    const page = await res.json();
    if (!Array.isArray(page) || page.length === 0) break;

    result.push(...page);
    if (page.length < LIMIT_PER_PAGE) break;

    const last = page[page.length - 1];
    if (!last.timestamp) break;
    after = last.timestamp;
  }

  return since ? result.filter(m => new Date(m.timestamp) > new Date(since)) : result;
}

// ------------------------------------------------------------
// GET LAST TIMESTAMP
// ------------------------------------------------------------
async function getLatestTimestamp(pool, locationId) {
  const { rows } = await pool.query(
    `SELECT timestamp FROM "${SUPABASE_TABLE}" WHERE location_id = $1 ORDER BY timestamp DESC LIMIT 1`,
    [locationId]
  );
  return rows.length ? rows[0].timestamp : null;
}

// ------------------------------------------------------------
// UPSERT FIXED COLUMNS
// ------------------------------------------------------------
async function upsertMeasurement(pool, m) {
  const sql = `
    INSERT INTO "${SUPABASE_TABLE}" (
      measurement_id, location_id, airPressure_Pa, airTemp_mK, autoValidation,
      deleted, manualValidation, timestamp, waterGround_mm, waterNAP_mm,
      waterPressure_Pa, waterTemp_mK, data, inserted_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),now()
    )
    ON CONFLICT (location_id, timestamp)
    DO UPDATE SET
      airPressure_Pa = EXCLUDED.airPressure_Pa,
      airTemp_mK = EXCLUDED.airTemp_mK,
      autoValidation = EXCLUDED.autoValidation,
      deleted = EXCLUDED.deleted,
      manualValidation = EXCLUDED.manualValidation,
      waterGround_mm = EXCLUDED.waterGround_mm,
      waterNAP_mm = EXCLUDED.waterNAP_mm,
      waterPressure_Pa = EXCLUDED.waterPressure_Pa,
      waterTemp_mK = EXCLUDED.waterTemp_mK,
      data = EXCLUDED.data,
      updated_at = now();
  `;

  const params = [
    m.id ?? null,
    m.locationId ?? m.location_id ?? m.location ?? null,
    m.airPressure_Pa ?? null,
    m.airTemp_mK ?? null,
    m.autoValidation ?? null,
    m.deleted ?? null,
    m.manualValidation ?? null,
    m.timestamp ?? m.time ?? null,
    m.waterGround_mm ?? null,
    m.waterNAP_mm ?? null,
    m.waterPressure_Pa ?? null,
    m.waterTemp_mK ?? null,
    m // store full JSON
  ];

  await pool.query(sql, params);
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

    const pool = new Pool({ connectionString: PG_SUPABASE_URL });
    let imported = 0;

    for (const loc of slice) {
      const locationId = loc.id ?? loc.locationId ?? loc.location_id;
      if (!locationId) continue;

      const lastTs = await getLatestTimestamp(pool, locationId);
      const measurements = await fetchMeasurements(token, locationId, lastTs);

      for (const m of measurements) {
        await upsertMeasurement(pool, m);
        imported++;
      }
    }

    await pool.end();

    const nextOffset = end < locations.length ? end : null;
    const nextUrl = nextOffset !== null ? `${req.url.split("?")[0]}?offset=${nextOffset}` : null;

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
