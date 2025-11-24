import os
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client
from time import sleep
import logging

# -----------------------------
# LOGGING CONFIG
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# CONFIG VIA ENVIRONMENT VARIABLES
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

REQUEST_SLEEP = float(os.environ.get("REQUEST_SLEEP", 0.1))

# -----------------------------
# CHECK ENV VARIABLES
# -----------------------------
required_envs = [
    SUPABASE_URL, SUPABASE_KEY, CLIENT_ID, CLIENT_SECRET
]
if not all(required_envs):
    logging.error("❌ Missing required environment variables. Exiting...")
    raise SystemExit("Missing required environment variables.")

# -----------------------------
# SUPABASE CLIENT
# -----------------------------
def supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# GLOBAL REQUEST SESSION
# -----------------------------
session = requests.Session()

# -----------------------------
# BATCH HELPER
# -----------------------------
def batch_list(lst, batch_size=50):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

# -----------------------------
# JWT TOKEN MANAGEMENT
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
    token = resp.json()["access_token"]
    expires_in = resp.json().get("expires_in", 3600)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    save_token(token, expires_at)
    logging.info("🟢 Nieuw JWT ontvangen en opgeslagen.")
    return token

def validate_token(jwt):
    logging.info("⏳ Token bestaat — controle of deze werkt...")
    test_url = f"{BASE_API_URL}/api/v3/locations"
    headers = {"Authorization": f"Bearer {jwt}"}
    resp = requests.get(test_url, headers=headers)
    if resp.status_code == 200:
        logging.info("🟢 JWT is geldig.")
        return True
    logging.warning(f"❌ JWT is ongeldig. Status: {resp.status_code}")
    return False

def get_valid_jwt():
    saved = load_most_recent_token()
    if not saved:
        logging.warning("❗ Geen token gevonden — nieuwe ophalen.")
        return request_new_jwt()
    jwt = saved["jwt"]
    expires_at = datetime.fromisoformat(saved["expires_at"])
    if expires_at < datetime.now(timezone.utc):
        logging.info("⏰ Token verlopen — vernieuwen.")
        return request_new_jwt()
    if validate_token(jwt):
        logging.info("♻️ Bestaande token blijft gebruikt.")
        return jwt
    logging.info("🔄 Token werkt niet — nieuwe aanvragen.")
    return request_new_jwt()

# -----------------------------
# SAFE GET UTILITY
# -----------------------------
def safe_get_ref_field(ref, key, idx=-1, as_type=None):
    if not ref:
        return None
    val = ref.get(key)
    if isinstance(val, list):
        if len(val) == 0:
            return None
        val = val[idx]
    if as_type:
        try:
            val = as_type(val)
        except (TypeError, ValueError):
            return None
    return val

def flatten_reference(location_id, name, lat, lon, ref):
    if not ref:
        return None
    if isinstance(ref, dict) and "timestamps" in ref:
        normalized = ref
    elif isinstance(ref, dict) and "referenceMeasurements" in ref:
        normalized = ref["referenceMeasurements"]
    elif isinstance(ref, list) and len(ref) > 0:
        normalized = ref[0]
    else:
        return None

    idx = -1
    def safe_int(val):
        try:
            if val is None:
                return None
            return int(round(float(val)))
        except (TypeError, ValueError):
            return None

    def safe_float(val):
        try:
            if val is None:
                return None
            return float(val)
        except (TypeError, ValueError):
            return None

    rec = {
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
    return rec

# -----------------------------
# LOCATION FETCH / STORE
# -----------------------------
def fetch_location_ids_from_api(token):
    logging.info("📡 Ophalen locatie-overzicht vanuit API...")
    url = f"{BASE_API_URL}/api/v2/locations"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    location_ids = [loc['id'] for loc in data]
    logging.info(f"✅ {len(location_ids)} locatie-id's opgehaald uit API")
    return location_ids, data

# -----------------------------
# V2 LOCATIONS
# -----------------------------
V2_MAPPING = {
    "notificationStatus": "notificationstatus",
    "maxGroundWaterLevel": "maxgroundwaterlevel",
    "currentGroundWaterLevel": "currentgroundwaterlevel",
    "deployments": "deployments",
    "minGroundWaterLevel": "mingroundwaterlevel",
    "currentAirPressurePa": "currentairpressurepa",
    "lastMeasurementAt": "lastmeasurementat",
    "currentTemperatureKelvin": "currenttemperaturekelvin",
    "latitude": "latitude",
    "id": "id",
    "description": "description",
    "address": "address",
    "currentBatteryVoltageVolts": "currentbatteryvoltagevolts",
    "name": "name",
    "lastMessageReceivedAt": "lastmessagereceivedat",
    "averageGroundWaterLevel": "averagegroundwaterlevel",
    "firmware": "firmware",
    "longitude": "longitude"
}

def fetch_v2_location_record(loc):
    return {supabase_col: loc.get(json_key) for json_key, supabase_col in V2_MAPPING.items()}

def store_v2_locations_batch(location_data, token, batch_size=50):
    sb = supabase_client()
    total_saved = 0
    for batch_idx, batch_locs in enumerate(batch_list(location_data, batch_size), start=1):
        records = [fetch_v2_location_record(loc) for loc in batch_locs]
        for rec in records:
            rec["updated_at"] = datetime.utcnow().isoformat()
        if records:
            sb.table(V2_LOCATION_TABLE).upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ Batch {batch_idx} opgeslagen ({len(records)} records) in V2")
        sleep(REQUEST_SLEEP)
    logging.info(f"🎉 Totaal {total_saved} v2 locaties opgeslagen!")

# -----------------------------
# V3 LOCATIONS
# -----------------------------
def flatten_v3_to_blik_location(loc):
    well = loc.get("well") or {}
    tube = well.get("tube") or {}
    tubeTop = tube.get("tubeTop") or {}
    plainTubePart = tube.get("plainTubePart") or {}
    screen = tube.get("screen") or {}
    sedimentSump = tube.get("sedimentSump") or {}
    material = tube.get("material") or {}

    record = {
        "id": loc.get("id"),
        "name": loc.get("name"),
        "internalname": loc.get("internalName"),
        "description": loc.get("description"),
        "address": loc.get("address"),
        "dataregimes": loc.get("dataRegimes"),
        "landowner_name": (loc.get("landOwner") or {}).get("name"),
        "landowner_kvk": (loc.get("landOwner") or {}).get("kvk"),
        "landowneragreements": loc.get("landOwnerAgreements"),
        "verticaldatum": loc.get("verticalDatum"),
        "groundlevel_position_mm": (loc.get("groundLevel") or {}).get("position_mm"),
        "groundlevel_positioningdate": (loc.get("groundLevel") or {}).get("positioningDate"),
        "groundlevel_positioningmethod": (loc.get("groundLevel") or {}).get("positioningMethod"),
        "groundlevel_stability": (loc.get("groundLevel") or {}).get("stability"),
        "well_horizontalposition_coordinates_system": (well.get("horizontalPosition") or {}).get("coordinates", {}).get("system"),
        "well_horizontalposition_coordinates_x": (well.get("horizontalPosition") or {}).get("coordinates", {}).get("x"),
        "well_horizontalposition_coordinates_y": (well.get("horizontalPosition") or {}).get("coordinates", {}).get("y"),
        "well_horizontalposition_positioningmethod": (well.get("horizontalPosition") or {}).get("positioningMethod"),
        "well_wellheadprotector": well.get("wellHeadProtector"),
        "well_lock": well.get("lock"),
        "well_owner_name": (well.get("owner") or {}).get("name"),
        "well_owner_kvk": (well.get("owner") or {}).get("kvk"),
        "well_deliveryaccountableparty_name": (well.get("deliveryAccountableParty") or {}).get("name"),
        "well_deliveryaccountableparty_kvk": (well.get("deliveryAccountableParty") or {}).get("kvk"),
        "well_deliveryresponsibleparty_name": (well.get("deliveryResponsibleParty") or {}).get("name"),
        "well_deliveryresponsibleparty_kvk": (well.get("deliveryResponsibleParty") or {}).get("kvk"),
        "well_maintenanceresponsibleparty_name": (well.get("maintenanceResponsibleParty") or {}).get("name"),
        "well_maintenanceresponsibleparty_kvk": (well.get("maintenanceResponsibleParty") or {}).get("kvk"),
        "well_deliverycontext": well.get("deliveryContext"),
        "well_constructionstandard": well.get("constructionStandard"),
        "well_initialfunction": well.get("initialFunction"),
        "well_constructiondate": well.get("constructionDate"),
        "well_removed": well.get("removed"),
        "well_removaldate": well.get("removalDate"),
        "well_stability": well.get("stability"),
        "well_externalidentifiers_nitgcode": (well.get("externalIdentifiers") or {}).get("nitgCode"),
        "well_externalidentifiers_broid": (well.get("externalIdentifiers") or {}).get("broId"),
        "well_tube_type": tube.get("type"),
        "well_tube_id": tube.get("id"),
        "well_tube_status": tube.get("status"),
        "well_tube_variablediameter": tube.get("variableDiameter"),
        "well_tube_cap": tube.get("cap"),
        "well_tube_material_tube": material.get("tube"),
        "well_tube_material_tubepacking": material.get("tubePacking"),
        "well_tube_material_glue": material.get("glue"),
        "well_tube_groundwaterflowjudgment": tube.get("groundWaterFlowJudgment"),
        "well_tube_tubetop_position_mm": tubeTop.get("position_mm"),
        "well_tube_tubetop_positioningdate": tubeTop.get("positioningDate"),
        "well_tube_tubetop_positioningmethod": tubeTop.get("positioningMethod"),
        "well_tube_tubetop_innerdiameter_mm": tubeTop.get("innerDiameter_mm"),
        "well_tube_tubetop_outerdiameter_mm": tubeTop.get("outerDiameter_mm"),
        "well_tube_plaintubepart_length_mm": plainTubePart.get("length_mm"),
        "well_tube_screen_length_mm": screen.get("length_mm"),
        "well_tube_screen_sockmaterial": screen.get("sockMaterial"),
        "well_tube_screen_protection": screen.get("protection"),
        "well_tube_sedimentsump_length_mm": sedimentSump.get("length_mm"),
        "updated_at": datetime.utcnow().isoformat()
    }
    return record

def store_v3_locations_to_blik_batch(location_ids, batch_size=50):
    token = get_valid_jwt()
    sb = supabase_client()
    total_records = 0

    for batch_idx, batch_ids in enumerate(batch_list(location_ids, batch_size), start=1):
        records = []
        logging.info(f"📦 Batch {batch_idx}: {len(batch_ids)} locaties ophalen...")
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
            sleep(REQUEST_SLEEP)

        if records:
            sb.table(LOCATION_DETAILS_TABLE).upsert(records).execute()
            total_records += len(records)
            logging.info(f"✅ Batch {batch_idx} opgeslagen ({len(records)} records) in V3")

    logging.info(f"🎉 Totaal {total_records} v3 locaties opgeslagen in {LOCATION_DETAILS_TABLE}!")

# -----------------------------
# REFERENTIEMETINGEN OPSLAAN
# -----------------------------
def fetch_reference_measurement(location_id, name, lat, lon, token):
    url = f"{BASE_API_URL}/api/v2/locations/{location_id}/reference-measurements?limit=1"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return flatten_reference(location_id, name, lat, lon, data)

def store_reference_measurements_batch(location_data, token, batch_size=50):
    sb = supabase_client()
    total_saved = 0

    for batch_idx, batch_locs in enumerate(batch_list(location_data, batch_size), start=1):
        logging.info(f"📦 Batch {batch_idx}: {len(batch_locs)} referentiemetingen ophalen...")
        records = []
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
                logging.warning(f"❌ Fout bij referentiemeting locatie {loc['id']}: {e}")
            sleep(REQUEST_SLEEP)

        if records:
            sb.table(REFERENCE_TABLE).upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ Batch {batch_idx} opgeslagen ({len(records)} records) in referentie")

    logging.info(f"🎉 Totaal {total_saved} referentiemetingen opgeslagen!")

# -----------------------------
# DEPLOYMENTS OPSLAAN
# -----------------------------
def store_location_deployments_flat_batch(location_ids, batch_size=50):
    token = get_valid_jwt()
    sb = supabase_client()
    total_saved = 0

    for batch_idx, batch_ids in enumerate(batch_list(location_ids, batch_size), start=1):
        records = []
        logging.info(f"📦 Batch {batch_idx}: {len(batch_ids)} locaties deployments ophalen...")

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
                                "flow_meter_pulse_amount_m3": d.get("flowMeterPulseAmountM3") if d else None,
                                "correction_type": corr.get("type") if corr else None,
                                "correction_value": corr.get("value") if corr else None,
                                "deleted": d.get("deleted") if d else None,
                                "sensor_type": sensor.get("type") if sensor else None,
                                "sensor_status": sensor.get("status") if sensor else None,
                                "created_at": datetime.utcnow().isoformat()
                            }
                            records.append(record)
                sleep(REQUEST_SLEEP)
            except Exception as e:
                logging.warning(f"❌ Fout bij ophalen deployments locatie {loc_id}: {e}")

        if records:
            sb.table("blik_serienummers").upsert(records).execute()
            total_saved += len(records)
            logging.info(f"✅ Batch {batch_idx} opgeslagen ({len(records)} deployment records)")

    logging.info(f"🎉 Totaal {total_saved} deployment records opgeslagen in blik_serienummers!")

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    token = get_valid_jwt()
    location_ids, location_data = fetch_location_ids_from_api(token)
    store_v2_locations_batch(location_data, token)
    store_v3_locations_to_blik_batch(location_ids)
    store_reference_measurements_batch(location_data, token)
    store_location_deployments_flat_batch(location_ids)
