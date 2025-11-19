// api/fetch-to-supabase.js
// Vercel serverless function using Supabase JS client
// Batch import + delta sync for BLIK_api

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js": "^2.36.0';

// -------------------------------
// Supabase client (service_role for INSERT/UPSERT)
// -------------------------------
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// -------------------------------
// Environment variables
// -------------------------------
const API_DOMAIN = process.env.API_DOMAIN;             
const AUTH_DOMAIN = process.env.AUTH_DOMAIN;           
const CLIENT_ID = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET;

const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1', 10);
const LIMIT_PER_PAGE = parseInt(process.env.LIMIT_PER_PAGE || '1000', 10);
const SUPABASE_TABLE = 'BLIK_api';
const METADATA_PATH = "/mnt/data/blik-metadata-v3.json";

// -------------------------------
// Auth0 Token
// -------------------------------
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

// -------------------------------
// Fetch locations
// -------------------------------
async function fetchLocations(token) {
  const res = await fetch(`https://${API_DOMAIN}/api/v3/locations`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) throw new Error(`Failed to load locations: ${res.status} ${await res.text()}`);
  const body = await res.json();
  return Array.isArray(body) ? body : Object.values(body);
}

// -------------------------------
// Fetch measurements
// -------------------------------
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

// -------------------------------
// Get latest timestamp for delta sync
// -------------------------------
async function getLatestTimestamp(locationId) {
  const { data, error } = await supabase
    .from(SUPABASE_TABLE)
    .select('timestamp')
    .eq('location_id', locationId)
    .order('timestamp', { ascending: false })
    .limit(1)
    .single();

  if (error && error.code !== 'PGRST116') console.error(error); // PGRST116 = no rows
  return data?.timestamp || null;
}

// -------------------------------
// Upsert measurement (fixed columns)
// -------------------------------
async function upsertMeasurement(m) {
  const record = {
    measurement_id: m.id ?? null,
    location_id: m.locationId ?? m.location_id ?? m.location ?? null,
    airPressure_Pa: m.airPressure_Pa ?? null,
    airTemp_mK: m.airTemp_mK ?? null,
    autoValidation: m.autoValidation ?? null,
    deleted: m.deleted ?? null,
    manualValidation: m.manualValidation ?? null,
    timestamp: m.timestamp ?? m.time ?? null,
    waterGround_mm: m.waterGround_mm ?? null,
    waterNAP_mm: m.waterNAP_mm ?? null,
    waterPressure_Pa: m.waterPressure_Pa ?? null,
    waterTemp_mK: m.waterTemp_mK ?? null,
    data: m
  };

  const { error } = await supabase
    .from(SUPABASE_TABLE)
    .upsert(record, { onConflict: ['location_id', 'timestamp'] });

  if (error) console.error('Upsert error:', error, 'Record:', record);
}

// -------------------------------
// Handler
// -------------------------------
export default async function handler(req, res) {
  try {
    const offset = parseInt(req.query.offset || '0', 10);
    const token = await getAccessToken();
    const locations = await fetchLocations(token);

    const start = offset;
    const end = Math.min(offset + BATCH_SIZE, locations.length);
    const slice = locations.slice(start, end);

    let imported = 0;

    for (const loc of slice) {
      const locationId = loc.id ?? loc.locationId ?? loc.location_id;
      if (!locationId) continue;

      const lastTs = await getLatestTimestamp(locationId);
      const measurements = await fetchMeasurements(token, locationId, lastTs);

      for (const m of measurements) {
        await upsertMeasurement(m);
        imported++;
      }
    }

    const nextOffset = end < locations.length ? end : null;
    const nextUrl = nextOffset !== null ? `${req.url.split('?')[0]}?offset=${nextOffset}` : null;

    res.status(200).json({
      success: true,
      batch: { start, end: end - 1 },
      imported,
      nextOffset,
      nextUrl,
      metadata_file: METADATA_PATH
    });

  } catch (err) {
    console.error('Handler error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
}
