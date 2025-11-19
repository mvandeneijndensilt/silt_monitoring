// api/fetch-to-supabase-full.js
// Fetch all BLIK measurements and upsert into Supabase

// TLS workaround (development only)
// Verwijder of commentaar in productie en gebruik correcte host met geldig cert
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';

// -------------------------------
// Supabase client
// -------------------------------
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const SUPABASE_TABLE = 'BLIK_api';

// -------------------------------
// Fixed config (mag zichtbaar)
// -------------------------------
const AUTH_DOMAIN = "blik.eu.auth0.com";
const API_DOMAIN = "water.bliksensing.nl"; // of lora.bliksensing.nl voor productie
const BATCH_SIZE = 10;       // kan verhogen bij stabielere omgeving
const LIMIT_PER_PAGE = 1000;
const CLIENT_ID = "ppiD46WfEm3i1R7cuQmSWHrhdXqWc96j";
const AUDIENCE = "https://water.bliksensing.nl";

// -------------------------------
// Secret from env
// -------------------------------
const CLIENT_SECRET = process.env.CLIENT_SECRET;

// -------------------------------
// Auth0 token request
// -------------------------------
async function getAccessToken() {
  const res = await fetch(`https://${AUTH_DOMAIN}/oauth/token`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      audience: AUDIENCE
    })
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Auth0 token request failed: ${res.status} ${text}`);
  }

  const data = await res.json();
  if (!data.access_token) throw new Error("No access_token returned from Auth0");
  return data.access_token;
}

// -------------------------------
// Fetch all locations
// -------------------------------
async function fetchLocations(token) {
  const res = await fetch(`https://${API_DOMAIN}/api/v3/locations`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok) throw new Error(`Failed to fetch locations: ${res.status} ${await res.text()}`);
  const body = await res.json();
  return Array.isArray(body) ? body : Object.values(body);
}

// -------------------------------
// Fetch all measurements for a location (no delta)
// -------------------------------
async function fetchMeasurements(token, locationId) {
  const results = [];
  let after = null;

  while (true) {
    const qs = new URLSearchParams({ limit: LIMIT_PER_PAGE });
    if (after) qs.set('after', after);

    const res = await fetch(`https://${API_DOMAIN}/api/v2/locations/${locationId}/measurements?${qs}`, {
      headers: { Authorization: `Bearer ${token}` }
    });

    if (!res.ok) throw new Error(`Failed measurements for location ${locationId}: ${res.status}`);

    const page = await res.json();
    if (!Array.isArray(page) || page.length === 0) break;

    results.push(...page);

    if (page.length < LIMIT_PER_PAGE) break;

    after = page[page.length - 1]?.timestamp;
    if (!after) break;
  }

  return results;
}

// -------------------------------
// Upsert measurement
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
// Vercel handler
// -------------------------------
export default async function handler(req, res) {
  try {
    console.log('Starting FULL BLIK sync...');
    const offset = parseInt(req.query.offset || '0', 10);

    const token = await getAccessToken();
    console.log('Got Auth0 token:', token.slice(0, 10) + '...');

    const locations = await fetchLocations(token);
    console.log('Fetched locations:', locations.length);

    const start = offset;
    const end = Math.min(offset + BATCH_SIZE, locations.length);
    const slice = locations.slice(start, end);

    let imported = 0;

    for (const loc of slice) {
      const locationId = loc.id ?? loc.locationId ?? loc.location_id;
      if (!locationId) continue;

      console.log('Processing location:', locationId);
      const measurements = await fetchMeasurements(token, locationId);

      for (const m of measurements) {
        await upsertMeasurement(m);
        imported++;
      }
    }

    const nextOffset = end < locations.length ? end : null;

    res.status(200).json({
      success: true,
      batch: { start, end: end - 1 },
      imported,
      nextOffset
    });

  } catch (err) {
    console.error('Function error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
}
