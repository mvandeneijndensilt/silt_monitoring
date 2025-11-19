// api/fetch-to-supabase-test.js
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const SUPABASE_TABLE = 'BLIK_api';

// Config
const AUTH_DOMAIN = "blik.eu.auth0.com";
const API_DOMAIN = "water.bliksensing.nl";
const CLIENT_ID = "ppiD46WfEm3i1R7cuQmSWHrhdXqWc96j";
const AUDIENCE = "https://water.bliksensing.nl";
const CLIENT_SECRET = process.env.CLIENT_SECRET;

// Auth0 token
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
  if (!res.ok) throw new Error(`Auth0 token request failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return data.access_token;
}

// Fetch locations
async function fetchLocations(token) {
  const res = await fetch(`https://${API_DOMAIN}/api/v3/locations`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const body = await res.json();
  return Array.isArray(body) ? body : Object.values(body);
}

// Fetch measurements safely
async function fetchMeasurements(token, locationId) {
  const res = await fetch(`https://${API_DOMAIN}/api/v3/locations/${locationId}/measurements?limit=5`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  const page = await res.json();
  console.log('Fetched measurements for location', locationId, page);
  if (!Array.isArray(page)) return [];
  return page;
}

// Upsert measurement dynamically
async function upsertMeasurement(m) {
  const record = {
    measurement_id: m.id ?? null,
    location_id: m.locationId ?? m.location_id ?? m.location ?? 1, // default 1 voor test
    timestamp: m.timestamp ?? m.time ?? new Date().toISOString(),
    data: m
  };

  const reserved = new Set(['id','locationId','location_id','location','timestamp','time']);
  Object.entries(m).forEach(([k,v]) => {
    if (!reserved.has(k)) record[k] = v;
  });

  const { error } = await supabase
    .from(SUPABASE_TABLE)
    .upsert(record, { onConflict: ['location_id', 'timestamp'] });

  if (error) console.error('Upsert error:', error, 'Record:', record);
}

// Vercel handler
export default async function handler(req, res) {
  try {
    const token = await getAccessToken();
    const locations = await fetchLocations(token);

    if (locations.length === 0) {
      return res.status(200).json({ success: true, message: 'No locations found' });
    }

    // Test: pak alleen de eerste locatie
    const loc = locations[0];
    const locationId = loc.id ?? loc.locationId ?? loc.location_id ?? 1;

    const measurements = await fetchMeasurements(token, locationId);

    let imported = 0;
    for (const m of measurements) {
      await upsertMeasurement(m);
      imported++;
    }

    // Voeg dummy record toe als er geen echte metingen zijn
    if (imported === 0) {
      await upsertMeasurement({ testValue: 123 });
      imported = 1;
    }

    res.status(200).json({
      success: true,
      locationId,
      imported,
      message: 'Test import completed'
    });

  } catch (err) {
    console.error('Test function error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
}
