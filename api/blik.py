import os
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client
import logging

# -----------------------------
# LOGGING CONFIG
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# ENV VARIABLES
# -----------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
AUDIENCE = os.environ.get("AUDIENCE", "https://water.bliksensing.nl")
TOKEN_URL = os.environ.get("TOKEN_URL", "https://blik.eu.auth0.com/oauth/token")
BASE_API_URL = os.environ.get("BASE_API_URL", "https://water-backend.blik.cloud")

TOKEN_TABLE = os.environ.get("TOKEN_TABLE", "api_tokens")
LOCATION_DETAILS_TABLE = os.environ.get("LOCATION_DETAILS_TABLE", "blik_location_v3")
REFERENCE_TABLE = os.environ.get("REFERENCE_TABLE", "blik_referentiemetingen")
V2_LOCATION_TABLE = os.environ.get("V2_LOCATION_TABLE", "blik_location_v2")

required_envs = [SUPABASE_URL, SUPABASE_KEY, CLIENT_ID, CLIENT_SECRET]
if not all(required_envs):
    logging.error("❌ Missing required environment variables.")
    raise SystemExit("Missing required environment variables.")

# -----------------------------
# SUPABASE CLIENT
# -----------------------------
def supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# BATCH HELPER
# -----------------------------
def batch_list(lst, batch_size=1):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

# -----------------------------
# TOKEN MANAGEMENT
# -----------------------------
def save_token(jwt, expires_at):
    sb = supabase_client()
    token_id = f"blik_api_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    sb.table(TOKEN_TABLE).insert({
        "id": token_id,
        "jwt": jwt,
        "expires_at": expires_at.isoformat(),
        "created_at": datetime.utcnow().isoformat()
    }).execute()
    logging.info(f"🔐 Nieuw token opgeslagen → {token_id}")
    return token_id

def load_most_recent_token():
    sb = supabase_client()
    res = sb.table(TOKEN_TABLE).select("*").order("created_at", desc=True).limit(1).execute()
    if res.data:
        logging.info("📦 Laatste token geladen uit database.")
        return res.data[0]
    return None

def request_new_jwt():
    logging.info("🔄 Nieuw JWT opvragen bij Auth0...")
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "audience": AUDIENCE,
        "grant_type": "client_credentials"
    }
    resp = requests.post(TOKEN_URL, json=payload)
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    expires_in = data.get("expires_in", 3600)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    save_token(token, expires_at)
    logging.info("🟢 Nieuw JWT ontvangen en opgeslagen.")
    return token

def validate_token(jwt):
    logging.info("⏳ Token controle...")
    test_url = f"{BASE_API_URL}/api/v3/locations"
    headers = {"Authorization": f"Bearer {jwt}"}
    try:
        resp = requests.get(test_url, headers=headers)
        return resp.status_code == 200
    except:
        return False

def get_valid_jwt():
    saved = load_most_recent_token()
    if saved:
        jwt = saved["jwt"]
        expires_at = datetime.fromisoformat(saved["expires_at"])
        if expires_at > datetime.now(timezone.utc) and validate_token(jwt):
            logging.info("♻️ Bestaande token geldig, gebruiken.")
            return jwt
        logging.info("⏰ Token verlopen of ongeldig, vernieuwen...")
    return request_new_jwt()

# -----------------------------
# SAFE GET
# -----------------------------
def safe_get_ref_field(ref, key, idx=-1, as_type=None):
    if not ref:
        return None
    val = ref.get(key)
    if isinstance(val, list):
        if not val:
            return None
        val = val[idx]
    if as_type:
        try:
            val = as_type(val)
        except:
            return None
    return val

# -----------------------------
# FLATTEN REFERENCE
# -----------------------------
def flatten_reference(location_id, name, lat, lon, ref):
    if not ref:
        return None
    if "timestamps" in ref:
        normalized = ref
    elif "referenceMeasurements" in ref:
        normalized = ref["referenceMeasurements"]
    elif isinstance(ref, list) and ref:
        normalized = ref[0]
    else:
        return None

    idx = -1
    def safe_int(v):
        try:
            return int(round(float(v))) if v is not None else None
        except:
            return None
    def safe_float(v):
        try:
            return float(v) if v is not None else None
        except:
            return None

    return {
        "location_id": safe_int(location_id),
        "name": name,
        "location_name": name,
        "timestamp": safe_int(safe_get_ref_field(normalized, "timestamps", idx)),
        "waterground_mm": safe_int(safe_get_ref_field(normalized, "waterGround_mm", idx)),
        "waternap_mm": safe_int(safe_get_ref_field(normalized, "waterNAP_mm", idx)),
        "validity": safe_get_ref_field(normalized, "validity", idx),
        "airpressure_pa": safe_float(safe_get_ref_field(normalized, "airPressure_Pa", idx)),
        "airtemp_mk": safe_float(safe_get_ref_field(normalized, "airTemp_mK", idx)),
        "waterpressure_pa": safe_float(safe_get_ref_field(normalized, "waterPressure_Pa", idx)),
        "watertemp_mk": safe_float(safe_get_ref_field(normalized, "waterTemp_mK", idx)),
        "pm25_ugm3": safe_float(safe_get_ref_field(normalized, "pm25_ugm3", idx)),
        "pm10_ugm3": safe_float(safe_get_ref_field(normalized, "pm10_ugm3", idx)),
        "no2_ugm3": safe_float(safe_get_ref_field(normalized, "no2_ugm3", idx)),
        "o3_ugm3": safe_float(safe_get_ref_field(normalized, "o3_ugm3", idx)),
        "airtemperature_c": safe_float(safe_get_ref_field(normalized, "airtemperature_c", idx)),
        "humidity_rh": safe_float(safe_get_ref_field(normalized, "humidity_rh", idx)),
        "windspeed_ms": safe_float(safe_get_ref_field(normalized, "windspeed_ms", idx)),
        "winddirection_deg": safe_float(safe_get_ref_field(normalized, "winddirection_deg", idx)),
        "rain_mm": safe_float(safe_get_ref_field(normalized, "rain_mm", idx)),
        "battery_v": safe_float(safe_get_ref_field(normalized, "battery_v", idx)),
        "latitude": safe_float(lat),
        "longitude": safe_float(lon),
        "updated_at": datetime.utcnow().isoformat()
    }

# -----------------------------
# FETCH LOCATIONS
# -----------------------------
def fetch_location_ids_from_api(token):
    logging.info("📡 Ophalen locatie-overzicht vanuit API...")
    url = f"{BASE_API_URL}/api/v2/locations"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    location_ids = [loc['id'] for loc in data]
    logging.info(f"✅ {len(location_ids)} locatie-id's opgehaald")
    return location_ids, data

# -----------------------------
# SERVERLESS HANDLER
# -----------------------------
def handler(event, context):
    try:
        token = get_valid_jwt()
        location_ids, location_data = fetch_location_ids_from_api(token)

        # V2
        store_v2_locations_batch(location_data, token, batch_size=1)
        # V3
        store_v3_locations_to_blik_batch(location_ids, batch_size=1)
        # Reference
        store_reference_measurements_batch(location_data, token, batch_size=1)
        # Deployments
        store_location_deployments_flat_batch(location_ids, batch_size=1)

        return {"statusCode": 200, "body": f"✅ {len(location_data)} locaties verwerkt"}
    except Exception as e:
        logging.error(f"❌ Error: {e}")
        return {"statusCode": 500, "body": f"❌ Error: {e}"}
# -----------------------------
# V2 LOCATIONS OPSLAAN
# -----------------------------
def store_v2_locations_batch(location_data, token, batch_size=5):
    sb = supabase_client()
    total_saved = 0
    for batch_idx, batch_locs in enumerate(batch_list(location_data, batch_size), start=1):
        records = []
        for loc in batch_locs:
            rec = {supabase_col: loc.get(json_key) for json_key, supabase_col in V2_MAPPING.items()}
            rec["updated_at"] = datetime.utcnow().isoformat()
            records.append(rec)

        if records:
            sb.table(V2_LOCATION_TABLE).upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ V2 batch {batch_idx} opgeslagen ({len(records)} records)")

    logging.info(f"🎉 Totaal {total_saved} V2 locaties opgeslagen!")

# -----------------------------
# V3 LOCATIONS OPSLAAN
# -----------------------------
def store_v3_locations_to_blik_batch(location_ids, batch_size=5):
    token = get_valid_jwt()
    sb = supabase_client()
    total_saved = 0

    for batch_idx, batch_ids in enumerate(batch_list(location_ids, batch_size), start=1):
        records = []
        logging.info(f"📦 V3 batch {batch_idx}: {len(batch_ids)} locaties ophalen...")

        for loc_id in batch_ids:
            try:
                url = f"{BASE_API_URL}/api/v3/locations/{loc_id}"
                headers = {"Authorization": f"Bearer {token}"}
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                loc_data = resp.json()
                records.append(flatten_v3_to_blik_location(loc_data))
            except Exception as e:
                logging.warning(f"❌ Fout bij locatie {loc_id}: {e}")

        if records:
            sb.table(LOCATION_DETAILS_TABLE).upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ V3 batch {batch_idx} opgeslagen ({len(records)} records)")

    logging.info(f"🎉 Totaal {total_saved} V3 locaties opgeslagen!")

# -----------------------------
# REFERENTIEMETINGEN OPSLAAN
# -----------------------------
def store_reference_measurements_batch(location_data, token, batch_size=5):
    sb = supabase_client()
    total_saved = 0

    for batch_idx, batch_locs in enumerate(batch_list(location_data, batch_size), start=1):
        records = []
        logging.info(f"📦 Referentie batch {batch_idx}: {len(batch_locs)} locaties...")

        for loc in batch_locs:
            try:
                rec = fetch_reference_measurement(
                    location_id=loc["id"],
                    name=loc.get("name"),
                    lat=loc.get("y"),
                    lon=loc.get("x"),
                    token=token
                )
                if rec:
                    records.append(rec)
            except Exception as e:
                logging.warning(f"❌ Fout referentiemeting locatie {loc['id']}: {e}")

        if records:
            sb.table(REFERENCE_TABLE).upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ Referentie batch {batch_idx} opgeslagen ({len(records)} records)")

    logging.info(f"🎉 Totaal {total_saved} referentiemetingen opgeslagen!")

# -----------------------------
# DEPLOYMENTS OPSLAAN
# -----------------------------
def store_location_deployments_flat_batch(location_ids, batch_size=5):
    token = get_valid_jwt()
    sb = supabase_client()
    total_saved = 0

    for batch_idx, batch_ids in enumerate(batch_list(location_ids, batch_size), start=1):
        records = []
        logging.info(f"📦 Deployment batch {batch_idx}: {len(batch_ids)} locaties...")

        for loc_id in batch_ids:
            try:
                url = f"{BASE_API_URL}/api/v3/locations/{loc_id}"
                headers = {"Authorization": f"Bearer {token}"}
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                loc_data = resp.json()
                deployments = loc_data.get("deployments") or [None]

                for d in deployments:
                    corrections = d.get("corrections") if d and d.get("corrections") else [None]
                    sensors = d.get("sensors") if d and d.get("sensors") else [None]

                    for corr in corrections:
                        for sensor in sensors:
                            record = {
                                "location_id": loc_id,
                                "node_id": d.get("nodeId") if d else None,
                                "node_serial": d.get("nodeSerial") if d else None,
                                "sensor_id": d.get("sensorId") if d else None,
                                "water_sensor_above_top_of_well_m": d.get("waterSensorAboveTopOfWell_m") if d else None,
                                "from_time": d.get("fromTime") if d else None,
                                "to_time": d.get("toTime") if d else None,
                                "flow_meter_coverage_m3": d.get("flowMeterCoverageM3") if d else None,
                                "flow_meter_green_is_in": d.get("flowMeterGreenIsIn") if d else None,
                                "flow_meter_pulse_amount_m3": d.get("FlowMeterPulseAmountM3") if d else None,
                                "correction_type": corr.get("type") if corr else None,
                                "correction_value": corr.get("value") if corr else None,
                                "deleted": d.get("deleted") if d else None,
                                "sensor_type": sensor.get("type") if sensor else None,
                                "sensor_status": sensor.get("status") if sensor else None,
                                "created_at": datetime.utcnow().isoformat()
                            }
                            records.append(record)
            except Exception as e:
                logging.warning(f"❌ Fout bij deployments locatie {loc_id}: {e}")

        if records:
            sb.table("blik_serienummers").upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ Deployment batch {batch_idx} opgeslagen ({len(records)} records)")

    logging.info(f"🎉 Totaal {total_saved} deployment records opgeslagen!")
