"""
Backfill 7 Days — Standalone Script
====================================
Pulls the last 7 full days of flight data from the OpenSky Network API,
loads raw data into a staging table, then builds separate Gold-layer tables
in the BACKFILL schema.

Does NOT modify any existing tables, DAGs, or dbt models.

Usage (from the codespace root):
    python backfill_7days.py

Requires the same .env variables used by the pipeline:
    OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET,
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
"""

import os
import logging
import time
from datetime import datetime, timedelta, timezone

import requests
import snowflake.connector
from dotenv import load_dotenv

# ── Load .env from project root ───────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────
OPENSKY_AUTH_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
OPENSKY_API_BASE = "https://opensky-network.org/api"
AIRPORTS = ["KJFK", "EGLL", "LFPG", "RJTT", "OMDB", "YSSY"]
DAYS_TO_BACKFILL = 7
API_PAUSE_SECONDS = 1

BACKFILL_SCHEMA = "BACKFILL"


# ── OpenSky helpers ────────────────────────────────────────────────────
def get_access_token():
    response = requests.post(
        OPENSKY_AUTH_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": os.environ["OPENSKY_CLIENT_ID"],
            "client_secret": os.environ["OPENSKY_CLIENT_SECRET"],
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    logger.info("OpenSky access token obtained.")
    return response.json()["access_token"]


def fetch_flights(token, airport, begin, end, direction):
    endpoint = f"{OPENSKY_API_BASE}/flights/{direction}"
    response = requests.get(
        endpoint,
        headers={"Authorization": f"Bearer {token}"},
        params={"airport": airport, "begin": begin, "end": end},
        timeout=60,
    )
    if response.status_code == 404:
        return []
    response.raise_for_status()
    flights = response.json()
    for f in flights:
        f["flight_direction"] = direction
        f["queried_airport"] = airport
    return flights


# ── Snowflake helpers ──────────────────────────────────────────────────
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )


def unix_to_timestamp(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ── Schema & table setup ──────────────────────────────────────────────
SETUP_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {BACKFILL_SCHEMA};

CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.raw_flights (
    icao24                                VARCHAR,
    first_seen                            TIMESTAMP_NTZ,
    est_departure_airport                 VARCHAR,
    last_seen                             TIMESTAMP_NTZ,
    est_arrival_airport                   VARCHAR,
    callsign                              VARCHAR,
    est_departure_airport_horiz_distance  INT,
    est_departure_airport_vert_distance   INT,
    est_arrival_airport_horiz_distance    INT,
    est_arrival_airport_vert_distance     INT,
    departure_airport_candidates_count    INT,
    arrival_airport_candidates_count      INT,
    flight_direction                      VARCHAR,
    queried_airport                       VARCHAR,
    ingested_at                           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.seed_airports (
    airport_icao      VARCHAR,
    airport_name      VARCHAR,
    city              VARCHAR,
    country           VARCHAR,
    latitude          FLOAT,
    longitude         FLOAT,
    timezone          VARCHAR,
    utc_offset_hours  FLOAT,
    is_tracked        BOOLEAN
);

CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.seed_airlines (
    callsign_prefix  VARCHAR,
    airline_name     VARCHAR,
    airline_iata     VARCHAR,
    airline_country  VARCHAR,
    alliance         VARCHAR
);
"""

INSERT_RAW_SQL = f"""
INSERT INTO {BACKFILL_SCHEMA}.raw_flights (
    icao24, first_seen, est_departure_airport, last_seen,
    est_arrival_airport, callsign,
    est_departure_airport_horiz_distance, est_departure_airport_vert_distance,
    est_arrival_airport_horiz_distance, est_arrival_airport_vert_distance,
    departure_airport_candidates_count, arrival_airport_candidates_count,
    flight_direction, queried_airport, ingested_at
) VALUES (
    %(icao24)s, %(first_seen)s, %(est_departure_airport)s, %(last_seen)s,
    %(est_arrival_airport)s, %(callsign)s,
    %(est_departure_airport_horiz_distance)s, %(est_departure_airport_vert_distance)s,
    %(est_arrival_airport_horiz_distance)s, %(est_arrival_airport_vert_distance)s,
    %(departure_airport_candidates_count)s, %(arrival_airport_candidates_count)s,
    %(flight_direction)s, %(queried_airport)s, %(ingested_at)s
);
"""

# ── Seed data ──────────────────────────────────────────────────────────
AIRPORTS_SEED = [
    ("KJFK", "John F Kennedy International", "New York", "United States", 40.6413, -73.7781, "America/New_York", -5, True),
    ("EGLL", "Heathrow", "London", "United Kingdom", 51.4700, -0.4543, "Europe/London", 0, True),
    ("LFPG", "Charles de Gaulle", "Paris", "France", 49.0097, 2.5479, "Europe/Paris", 1, True),
    ("RJTT", "Haneda", "Tokyo", "Japan", 35.5494, 139.7798, "Asia/Tokyo", 9, True),
    ("OMDB", "Dubai International", "Dubai", "United Arab Emirates", 25.2532, 55.3657, "Asia/Dubai", 4, True),
    ("YSSY", "Sydney Kingsford Smith", "Sydney", "Australia", -33.9461, 151.1772, "Australia/Sydney", 11, True),
    ("YMML", "Melbourne Tullamarine", "Melbourne", "Australia", -37.6690, 144.8410, "Australia/Melbourne", 11, False),
    ("RJFF", "Fukuoka", "Fukuoka", "Japan", 33.5859, 130.4511, "Asia/Tokyo", 9, False),
    ("YBBN", "Brisbane", "Brisbane", "Australia", -27.3842, 153.1175, "Australia/Brisbane", 10, False),
    ("KLAX", "Los Angeles International", "Los Angeles", "United States", 33.9425, -118.4081, "America/Los_Angeles", -8, False),
    ("RJCC", "New Chitose", "Sapporo", "Japan", 42.7752, 141.6924, "Asia/Tokyo", 9, False),
    ("EDDF", "Frankfurt", "Frankfurt", "Germany", 50.0379, 8.5622, "Europe/Berlin", 1, False),
    ("EHAM", "Schiphol", "Amsterdam", "Netherlands", 52.3086, 4.7639, "Europe/Amsterdam", 1, False),
    ("RJOO", "Itami", "Osaka", "Japan", 34.7855, 135.4381, "Asia/Tokyo", 9, False),
    ("LEMD", "Adolfo Suarez Madrid-Barajas", "Madrid", "Spain", 40.4936, -3.5668, "Europe/Madrid", 1, False),
    ("EDDM", "Munich", "Munich", "Germany", 48.3538, 11.7861, "Europe/Berlin", 1, False),
    ("EIDW", "Dublin", "Dublin", "Ireland", 53.4213, -6.2701, "Europe/Dublin", 0, False),
    ("LSGG", "Geneva", "Geneva", "Switzerland", 46.2381, 6.1089, "Europe/Zurich", 1, False),
    ("KSFO", "San Francisco International", "San Francisco", "United States", 37.6213, -122.3790, "America/Los_Angeles", -8, False),
    ("ROAH", "Naha", "Okinawa", "Japan", 26.1958, 127.6459, "Asia/Tokyo", 9, False),
    ("KBOS", "Logan International", "Boston", "United States", 42.3656, -71.0096, "America/New_York", -5, False),
    ("LSZH", "Zurich", "Zurich", "Switzerland", 47.4647, 8.5492, "Europe/Zurich", 1, False),
    ("LEBL", "El Prat", "Barcelona", "Spain", 41.2971, 2.0785, "Europe/Madrid", 1, False),
    ("EKCH", "Kastrup", "Copenhagen", "Denmark", 55.6181, 12.6561, "Europe/Copenhagen", 1, False),
    ("LIRF", "Fiumicino", "Rome", "Italy", 41.8003, 12.2389, "Europe/Rome", 1, False),
    ("LTFM", "Istanbul", "Istanbul", "Turkey", 41.2608, 28.7419, "Europe/Istanbul", 3, False),
    ("EGCC", "Manchester", "Manchester", "United Kingdom", 53.3537, -2.2750, "Europe/London", 0, False),
    ("YPAD", "Adelaide", "Adelaide", "Australia", -34.9450, 138.5306, "Australia/Adelaide", 10.5, False),
    ("VHHH", "Hong Kong International", "Hong Kong", "China", 22.3080, 113.9185, "Asia/Hong_Kong", 8, False),
    ("EGPH", "Edinburgh", "Edinburgh", "United Kingdom", 55.9508, -3.3725, "Europe/London", 0, False),
    ("YBCG", "Gold Coast", "Gold Coast", "Australia", -28.1644, 153.5047, "Australia/Brisbane", 10, False),
    ("LIMC", "Malpensa", "Milan", "Italy", 45.6306, 8.7231, "Europe/Rome", 1, False),
    ("LIML", "Linate", "Milan", "Italy", 45.4456, 9.2778, "Europe/Rome", 1, False),
    ("LOWW", "Vienna International", "Vienna", "Austria", 48.1103, 16.5697, "Europe/Vienna", 1, False),
    ("YSCB", "Canberra", "Canberra", "Australia", -35.3069, 149.1946, "Australia/Sydney", 11, False),
    ("KATL", "Hartsfield-Jackson", "Atlanta", "United States", 33.6407, -84.4277, "America/New_York", -5, False),
    ("EDDB", "Berlin Brandenburg", "Berlin", "Germany", 52.3667, 13.5033, "Europe/Berlin", 1, False),
    ("KMIA", "Miami International", "Miami", "United States", 25.7959, -80.2870, "America/New_York", -5, False),
    ("ESSA", "Arlanda", "Stockholm", "Sweden", 59.6519, 17.9186, "Europe/Stockholm", 1, False),
    ("CYYZ", "Pearson International", "Toronto", "Canada", 43.6777, -79.6248, "America/Toronto", -5, False),
    ("NZAA", "Auckland", "Auckland", "New Zealand", -37.0082, 174.7850, "Pacific/Auckland", 13, False),
    ("EPWA", "Chopin", "Warsaw", "Poland", 52.1657, 20.9671, "Europe/Warsaw", 1, False),
    ("YPPH", "Perth", "Perth", "Australia", -31.9403, 115.9670, "Australia/Perth", 8, False),
    ("RJCJ", "Chitose Air Base", "Chitose", "Japan", 42.7945, 141.6664, "Asia/Tokyo", 9, False),
    ("RJBB", "Kansai International", "Osaka", "Japan", 34.4347, 135.2440, "Asia/Tokyo", 9, False),
    ("EDDL", "Dusseldorf", "Dusseldorf", "Germany", 51.2895, 6.7668, "Europe/Berlin", 1, False),
    ("KORD", "O Hare International", "Chicago", "United States", 41.9742, -87.9073, "America/Chicago", -6, False),
    ("LFBO", "Blagnac", "Toulouse", "France", 43.6291, 1.3638, "Europe/Paris", 1, False),
    ("LIPZ", "Marco Polo", "Venice", "Italy", 45.5053, 12.3519, "Europe/Rome", 1, False),
    ("VTBS", "Suvarnabhumi", "Bangkok", "Thailand", 13.6900, 100.7501, "Asia/Bangkok", 7, False),
    ("ENGM", "Gardermoen", "Oslo", "Norway", 60.1939, 11.1004, "Europe/Oslo", 1, False),
    ("RJOT", "Takamatsu", "Takamatsu", "Japan", 34.2142, 134.0156, "Asia/Tokyo", 9, False),
    ("EDDH", "Hamburg", "Hamburg", "Germany", 53.6304, 9.9882, "Europe/Berlin", 1, False),
    ("EGPF", "Glasgow", "Glasgow", "United Kingdom", 55.8642, -4.4331, "Europe/London", 0, False),
    ("LKPR", "Vaclav Havel", "Prague", "Czech Republic", 50.1008, 14.2600, "Europe/Prague", 1, False),
    ("KEWR", "Newark Liberty", "Newark", "United States", 40.6895, -74.1745, "America/New_York", -5, False),
    ("LFML", "Marseille Provence", "Marseille", "France", 43.4393, 5.2214, "Europe/Paris", 1, False),
    ("RKSS", "Gimpo International", "Seoul", "South Korea", 37.5586, 126.7906, "Asia/Seoul", 9, False),
    ("VIDP", "Indira Gandhi International", "New Delhi", "India", 28.5562, 77.1000, "Asia/Kolkata", 5.5, False),
    ("KDFW", "Dallas Fort Worth", "Dallas", "United States", 32.8998, -97.0403, "America/Chicago", -6, False),
    ("EFHK", "Helsinki-Vantaa", "Helsinki", "Finland", 60.3172, 24.9633, "Europe/Helsinki", 2, False),
    ("LPPT", "Lisbon Portela", "Lisbon", "Portugal", 38.7756, -9.1354, "Europe/Lisbon", 0, False),
    ("LGAV", "Eleftherios Venizelos", "Athens", "Greece", 37.9364, 23.9445, "Europe/Athens", 2, False),
    ("KSEA", "Seattle-Tacoma", "Seattle", "United States", 47.4502, -122.3088, "America/Los_Angeles", -8, False),
    ("LIPE", "Guglielmo Marconi", "Bologna", "Italy", 44.5354, 11.2887, "Europe/Rome", 1, False),
    ("WMKK", "Kuala Lumpur International", "Kuala Lumpur", "Malaysia", 2.7456, 101.7099, "Asia/Kuala_Lumpur", 8, False),
    ("LFLL", "Saint-Exupery", "Lyon", "France", 45.7256, 5.0811, "Europe/Paris", 1, False),
    ("WSAP", "Changi", "Singapore", "Singapore", 1.3502, 103.9940, "Asia/Singapore", 8, False),
    ("RJFT", "Kumamoto", "Kumamoto", "Japan", 32.8373, 130.8551, "Asia/Tokyo", 9, False),
    ("KIAD", "Washington Dulles", "Washington DC", "United States", 38.9531, -77.4565, "America/New_York", -5, False),
    ("RCSS", "Songshan", "Taipei", "Taiwan", 25.0694, 121.5525, "Asia/Taipei", 8, False),
    ("KLAS", "McCarran International", "Las Vegas", "United States", 36.0840, -115.1537, "America/Los_Angeles", -8, False),
    ("LROP", "Henri Coanda", "Bucharest", "Romania", 44.5711, 26.0850, "Europe/Bucharest", 2, False),
    ("KFLL", "Fort Lauderdale-Hollywood", "Fort Lauderdale", "United States", 26.0726, -80.1527, "America/New_York", -5, False),
    ("EGPD", "Aberdeen", "Aberdeen", "United Kingdom", 57.2019, -2.1978, "Europe/London", 0, False),
    ("EGNT", "Newcastle", "Newcastle", "United Kingdom", 55.0374, -1.6917, "Europe/London", 0, False),
    ("LHBP", "Liszt Ferenc", "Budapest", "Hungary", 47.4369, 19.2556, "Europe/Budapest", 1, False),
    ("RJBE", "Kobe", "Kobe", "Japan", 34.6328, 135.2240, "Asia/Tokyo", 9, False),
    ("CYUL", "Trudeau International", "Montreal", "Canada", 45.4706, -73.7408, "America/Montreal", -5, False),
    ("LFBD", "Merignac", "Bordeaux", "France", 44.8283, -0.7156, "Europe/Paris", 1, False),
    ("KIAH", "George Bush Intercontinental", "Houston", "United States", 29.9902, -95.3368, "America/Chicago", -6, False),
    ("RPLL", "Ninoy Aquino International", "Manila", "Philippines", 14.5086, 121.0198, "Asia/Manila", 8, False),
    ("RCTP", "Taoyuan International", "Taipei", "Taiwan", 25.0777, 121.2325, "Asia/Taipei", 8, False),
    ("SBGR", "Guarulhos International", "Sao Paulo", "Brazil", -23.4356, -46.4731, "America/Sao_Paulo", -3, False),
    ("CYVR", "Vancouver International", "Vancouver", "Canada", 49.1947, -123.1839, "America/Vancouver", -8, False),
    ("BIKF", "Keflavik International", "Reykjavik", "Iceland", 63.9850, -22.6056, "Atlantic/Reykjavik", 0, False),
    ("RJAA", "Narita International", "Tokyo", "Japan", 35.7647, 140.3864, "Asia/Tokyo", 9, False),
    ("WSSS", "Changi International", "Singapore", "Singapore", 1.3644, 103.9915, "Asia/Singapore", 8, False),
    ("FAOR", "OR Tambo International", "Johannesburg", "South Africa", -26.1392, 28.2460, "Africa/Johannesburg", 2, False),
    ("NZCH", "Christchurch International", "Christchurch", "New Zealand", -43.4894, 172.5322, "Pacific/Auckland", 13, False),
    ("NZWN", "Wellington International", "Wellington", "New Zealand", -41.3272, 174.8053, "Pacific/Auckland", 13, False),
    ("SAEZ", "Ministro Pistarini", "Buenos Aires", "Argentina", -34.8222, -58.5358, "America/Argentina/Buenos_Aires", -3, False),
    ("FACT", "Cape Town International", "Cape Town", "South Africa", -33.9649, 18.6017, "Africa/Johannesburg", 2, False),
]

AIRLINES_SEED = [
    ("AAL", "American Airlines", "AA", "United States", "oneworld"),
    ("AAR", "Asiana Airlines", "OZ", "South Korea", "Star Alliance"),
    ("ACA", "Air Canada", "AC", "Canada", "Star Alliance"),
    ("AFR", "Air France", "AF", "France", "SkyTeam"),
    ("AIC", "Air India", "AI", "India", "Star Alliance"),
    ("AIJ", "Interjet", "4O", "Mexico", "none"),
    ("AJX", "Air Japan", "NQ", "Japan", "none"),
    ("ANA", "All Nippon Airways", "NH", "Japan", "Star Alliance"),
    ("ANZ", "Air New Zealand", "NZ", "New Zealand", "Star Alliance"),
    ("APJ", "Peach Aviation", "MM", "Japan", "none"),
    ("ASA", "Alaska Airlines", "AS", "United States", "oneworld"),
    ("AUA", "Austrian Airlines", "OS", "Austria", "Star Alliance"),
    ("AVA", "Avianca", "AV", "Colombia", "Star Alliance"),
    ("AZA", "Alitalia / ITA Airways", "AZ", "Italy", "SkyTeam"),
    ("BAW", "British Airways", "BA", "United Kingdom", "oneworld"),
    ("BEE", "Flybe", "BE", "United Kingdom", "none"),
    ("BEL", "Brussels Airlines", "SN", "Belgium", "Star Alliance"),
    ("BER", "Air Berlin", "AB", "Germany", "none"),
    ("CAL", "China Airlines", "CI", "Taiwan", "SkyTeam"),
    ("CCA", "Air China", "CA", "China", "Star Alliance"),
    ("CES", "China Eastern", "MU", "China", "SkyTeam"),
    ("CLH", "Lufthansa CityLine", "CL", "Germany", "Star Alliance"),
    ("CPA", "Cathay Pacific", "CX", "Hong Kong", "oneworld"),
    ("CSN", "China Southern", "CZ", "China", "SkyTeam"),
    ("DAL", "Delta Air Lines", "DL", "United States", "SkyTeam"),
    ("DLH", "Lufthansa", "LH", "Germany", "Star Alliance"),
    ("EIN", "Aer Lingus", "EI", "Ireland", "none"),
    ("EJU", "easyJet Europe", "EC", "Austria", "none"),
    ("ETD", "Etihad Airways", "EY", "United Arab Emirates", "none"),
    ("ETH", "Ethiopian Airlines", "ET", "Ethiopia", "Star Alliance"),
    ("EVA", "EVA Air", "BR", "Taiwan", "Star Alliance"),
    ("EWG", "Eurowings", "EW", "Germany", "none"),
    ("EZY", "easyJet", "U2", "United Kingdom", "none"),
    ("FDX", "FedEx Express", "FX", "United States", "none"),
    ("FIN", "Finnair", "AY", "Finland", "oneworld"),
    ("GEC", "Lufthansa Cargo", "GE", "Germany", "Star Alliance"),
    ("IBE", "Iberia", "IB", "Spain", "oneworld"),
    ("IBK", "Norwegian Air International", "D8", "Ireland", "none"),
    ("ICE", "Icelandair", "FI", "Iceland", "none"),
    ("JAL", "Japan Airlines", "JL", "Japan", "oneworld"),
    ("JBU", "JetBlue Airways", "B6", "United States", "none"),
    ("JJA", "Jeju Air", "7C", "South Korea", "none"),
    ("JST", "Jetstar Airways", "JQ", "Australia", "none"),
    ("KAL", "Korean Air", "KE", "South Korea", "SkyTeam"),
    ("KLM", "KLM Royal Dutch", "KL", "Netherlands", "SkyTeam"),
    ("LOT", "LOT Polish Airlines", "LO", "Poland", "Star Alliance"),
    ("NAX", "Norwegian Air Shuttle", "DY", "Norway", "none"),
    ("NCA", "Nippon Cargo Airlines", "KZ", "Japan", "none"),
    ("QFA", "Qantas", "QF", "Australia", "oneworld"),
    ("QTR", "Qatar Airways", "QR", "Qatar", "oneworld"),
    ("RYR", "Ryanair", "FR", "Ireland", "none"),
    ("RZO", "SCAT Airlines", "DV", "Kazakhstan", "none"),
    ("SAS", "Scandinavian Airlines", "SK", "Scandinavia", "Star Alliance"),
    ("SIA", "Singapore Airlines", "SQ", "Singapore", "Star Alliance"),
    ("SKW", "SkyWest Airlines", "OO", "United States", "none"),
    ("SNO", "SkyNorway", "SN", "Norway", "none"),
    ("SWA", "Southwest Airlines", "WN", "United States", "none"),
    ("SWR", "Swiss International", "LX", "Switzerland", "Star Alliance"),
    ("TAP", "TAP Air Portugal", "TP", "Portugal", "Star Alliance"),
    ("THA", "Thai Airways", "TG", "Thailand", "Star Alliance"),
    ("THY", "Turkish Airlines", "TK", "Turkey", "Star Alliance"),
    ("TUI", "TUI Airways", "BY", "United Kingdom", "none"),
    ("TVF", "Transavia France", "TO", "France", "none"),
    ("UAE", "Emirates", "EK", "United Arab Emirates", "none"),
    ("UAL", "United Airlines", "UA", "United States", "Star Alliance"),
    ("UPS", "UPS Airlines", "5X", "United States", "none"),
    ("VIR", "Virgin Atlantic", "VS", "United Kingdom", "none"),
    ("VOZ", "Virgin Australia", "VA", "Australia", "none"),
    ("VJC", "VietJet Air", "VJ", "Vietnam", "none"),
    ("VLG", "Vueling", "VY", "Spain", "none"),
    ("WZZ", "Wizz Air", "W6", "Hungary", "none"),
    # ── Previously unmatched airlines (from data quality check) ──
    ("EDV", "Endeavor Air", "9E", "United States", "SkyTeam"),
    ("QLK", "QantasLink", "QF", "Australia", "oneworld"),
    ("SHT", "British Airways Shuttle", "BA", "United Kingdom", "oneworld"),
    ("SKY", "Skymark Airlines", "BC", "Japan", "none"),
    ("RPA", "Republic Airways", "YX", "United States", "none"),
    ("SNJ", "Solaseed Air", "6J", "Japan", "none"),
    ("SFJ", "StarFlyer", "7G", "Japan", "none"),
    ("ADO", "Air Do", "HD", "Japan", "none"),
    ("RXA", "Rex Airlines", "ZL", "Australia", "none"),
    ("TAY", "TNT Airways", "3V", "Belgium", "none"),
    ("DAH", "Air Algerie", "AH", "Algeria", "none"),
    ("ITY", "ITA Airways", "AZ", "Italy", "SkyTeam"),
    ("SVA", "Saudia", "SV", "Saudi Arabia", "SkyTeam"),
    ("ELY", "El Al", "LY", "Israel", "none"),
    ("MAS", "Malaysia Airlines", "MH", "Malaysia", "oneworld"),
    ("BCS", "European Air Transport", "QY", "Belgium", "none"),
    ("AM2", "Aeromexico Connect", "5D", "Mexico", "SkyTeam"),
    ("FPO", "ASL Airlines France", "5O", "France", "none"),
    ("LOG", "Loganair", "LM", "United Kingdom", "none"),
    ("LHX", "Lufthansa Global Business Aviation", "LH", "Germany", "Star Alliance"),
]


# ── Gold table SQL (mirrors dbt models, reads from BACKFILL schema) ──

GOLD_DAILY_AIRPORT_TRAFFIC = f"""
CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.gld_daily_airport_traffic AS

WITH flights AS (
    SELECT
        icao24                                          AS aircraft_icao24,
        TRIM(callsign)                                  AS callsign,
        est_departure_airport                           AS departure_airport_icao,
        est_arrival_airport                             AS arrival_airport_icao,
        first_seen                                      AS departed_at,
        last_seen                                       AS arrived_at,
        TIMEDIFF('minute', first_seen, last_seen)       AS flight_duration_minutes,
        UPPER(LEFT(TRIM(callsign), 3))                  AS callsign_prefix,
        flight_direction,
        queried_airport,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, first_seen, last_seen, flight_direction
            ORDER BY ingested_at DESC
        ) AS _row_num
    FROM {BACKFILL_SCHEMA}.raw_flights
    WHERE (est_departure_airport IS NOT NULL OR est_arrival_airport IS NOT NULL)
      AND first_seen <= last_seen
),

clean AS (
    SELECT * FROM flights WHERE _row_num = 1
),

arrivals AS (
    SELECT
        arrived_at::DATE                    AS flight_date,
        arrival_airport_icao                AS airport_icao,
        COUNT(*)                            AS total_arrivals,
        AVG(flight_duration_minutes)        AS avg_arrival_duration_min,
        MIN(flight_duration_minutes)        AS min_arrival_duration_min,
        MAX(flight_duration_minutes)        AS max_arrival_duration_min
    FROM clean
    WHERE flight_direction = 'arrival'
      AND arrival_airport_icao IS NOT NULL
    GROUP BY 1, 2
),

departures AS (
    SELECT
        departed_at::DATE                   AS flight_date,
        departure_airport_icao              AS airport_icao,
        COUNT(*)                            AS total_departures,
        AVG(flight_duration_minutes)        AS avg_departure_duration_min
    FROM clean
    WHERE flight_direction = 'departure'
      AND departure_airport_icao IS NOT NULL
    GROUP BY 1, 2
),

combined AS (
    SELECT
        COALESCE(a.flight_date, d.flight_date)      AS flight_date,
        COALESCE(a.airport_icao, d.airport_icao)     AS airport_icao,
        COALESCE(a.total_arrivals, 0)                AS total_arrivals,
        COALESCE(d.total_departures, 0)              AS total_departures,
        COALESCE(a.total_arrivals, 0)
            + COALESCE(d.total_departures, 0)        AS total_movements,
        a.avg_arrival_duration_min,
        a.min_arrival_duration_min,
        a.max_arrival_duration_min,
        d.avg_departure_duration_min
    FROM arrivals a
    FULL OUTER JOIN departures d
        ON a.flight_date = d.flight_date
        AND a.airport_icao = d.airport_icao
)

SELECT
    c.*,
    ap.airport_name,
    ap.city,
    ap.country
FROM combined c
LEFT JOIN {BACKFILL_SCHEMA}.seed_airports ap
    ON c.airport_icao = ap.airport_icao;
"""

GOLD_ROUTE_ANALYSIS = f"""
CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.gld_route_analysis AS

WITH flights AS (
    SELECT
        icao24                                          AS aircraft_icao24,
        est_departure_airport                           AS departure_airport_icao,
        est_arrival_airport                             AS arrival_airport_icao,
        first_seen                                      AS departed_at,
        last_seen                                       AS arrived_at,
        TIMEDIFF('minute', first_seen, last_seen)       AS flight_duration_minutes,
        flight_direction,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, first_seen, last_seen, flight_direction
            ORDER BY ingested_at DESC
        ) AS _row_num
    FROM {BACKFILL_SCHEMA}.raw_flights
    WHERE (est_departure_airport IS NOT NULL OR est_arrival_airport IS NOT NULL)
      AND first_seen <= last_seen
),

clean AS (
    SELECT * FROM flights WHERE _row_num = 1
),

routes AS (
    SELECT
        departure_airport_icao,
        arrival_airport_icao,
        COUNT(*)                            AS total_flights,
        AVG(flight_duration_minutes)        AS avg_duration_min,
        MIN(flight_duration_minutes)        AS min_duration_min,
        MAX(flight_duration_minutes)        AS max_duration_min,
        COUNT(DISTINCT aircraft_icao24)     AS unique_aircraft,
        COUNT(DISTINCT departed_at::DATE)   AS days_with_service,
        MIN(departed_at::DATE)              AS first_seen_date,
        MAX(departed_at::DATE)              AS last_seen_date
    FROM clean
    WHERE departure_airport_icao IS NOT NULL
      AND arrival_airport_icao IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    r.*,
    dep.airport_name    AS departure_airport_name,
    dep.city            AS departure_city,
    dep.country         AS departure_country,
    arr.airport_name    AS arrival_airport_name,
    arr.city            AS arrival_city,
    arr.country         AS arrival_country
FROM routes r
LEFT JOIN {BACKFILL_SCHEMA}.seed_airports dep
    ON r.departure_airport_icao = dep.airport_icao
LEFT JOIN {BACKFILL_SCHEMA}.seed_airports arr
    ON r.arrival_airport_icao = arr.airport_icao;
"""

GOLD_HOURLY_FLIGHT_ACTIVITY = f"""
CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.gld_hourly_flight_activity AS

WITH flights AS (
    SELECT
        first_seen                                      AS departed_at,
        TIMEDIFF('minute', first_seen, last_seen)       AS flight_duration_minutes,
        queried_airport                                 AS airport_icao,
        flight_direction,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, first_seen, last_seen, flight_direction
            ORDER BY ingested_at DESC
        ) AS _row_num
    FROM {BACKFILL_SCHEMA}.raw_flights
    WHERE (est_departure_airport IS NOT NULL OR est_arrival_airport IS NOT NULL)
      AND first_seen <= last_seen
),

clean AS (
    SELECT * FROM flights WHERE _row_num = 1
),

hourly AS (
    SELECT
        departed_at::DATE                           AS flight_date,
        HOUR(departed_at)                           AS departure_hour_utc,
        airport_icao,
        flight_direction,
        COUNT(*)                                    AS flight_count,
        AVG(flight_duration_minutes)                AS avg_duration_min
    FROM clean
    GROUP BY 1, 2, 3, 4
)

SELECT
    h.*,
    MOD(h.departure_hour_utc + ap.utc_offset_hours + 24, 24)  AS departure_hour_local,
    ap.airport_name,
    ap.city,
    ap.country,
    ap.timezone
FROM hourly h
LEFT JOIN {BACKFILL_SCHEMA}.seed_airports ap
    ON h.airport_icao = ap.airport_icao;
"""

GOLD_AIRLINE_TRAFFIC = f"""
CREATE OR REPLACE TABLE {BACKFILL_SCHEMA}.gld_airline_traffic AS

WITH flights AS (
    SELECT
        icao24                                          AS aircraft_icao24,
        TRIM(callsign)                                  AS callsign,
        UPPER(LEFT(TRIM(callsign), 3))                  AS callsign_prefix,
        first_seen                                      AS departed_at,
        TIMEDIFF('minute', first_seen, last_seen)       AS flight_duration_minutes,
        queried_airport,
        flight_direction,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, first_seen, last_seen, flight_direction
            ORDER BY ingested_at DESC
        ) AS _row_num
    FROM {BACKFILL_SCHEMA}.raw_flights
    WHERE (est_departure_airport IS NOT NULL OR est_arrival_airport IS NOT NULL)
      AND first_seen <= last_seen
),

clean AS (
    SELECT * FROM flights WHERE _row_num = 1
),

airline_stats AS (
    SELECT
        callsign_prefix,
        queried_airport                             AS airport_icao,
        flight_direction,
        COUNT(*)                                    AS total_flights,
        COUNT(DISTINCT aircraft_icao24)             AS unique_aircraft,
        COUNT(DISTINCT departed_at::DATE)           AS days_active,
        ROUND(AVG(flight_duration_minutes), 1)      AS avg_duration_min,
        MIN(departed_at::DATE)                      AS first_seen_date,
        MAX(departed_at::DATE)                      AS last_seen_date
    FROM clean
    WHERE callsign_prefix IS NOT NULL
      AND callsign_prefix != ''
    GROUP BY 1, 2, 3
)

SELECT
    s.*,
    al.airline_name,
    al.airline_iata,
    al.airline_country,
    al.alliance,
    ap.airport_name,
    ap.city,
    ap.country AS airport_country
FROM airline_stats s
LEFT JOIN {BACKFILL_SCHEMA}.seed_airlines al
    ON s.callsign_prefix = al.callsign_prefix
LEFT JOIN {BACKFILL_SCHEMA}.seed_airports ap
    ON s.airport_icao = ap.airport_icao;
"""


# ── Data loading helpers ───────────────────────────────────────────────
def load_raw_flights(conn, flights):
    if not flights:
        return 0
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for f in flights:
        rows.append({
            "icao24": f.get("icao24"),
            "first_seen": unix_to_timestamp(f.get("firstSeen")),
            "est_departure_airport": f.get("estDepartureAirport"),
            "last_seen": unix_to_timestamp(f.get("lastSeen")),
            "est_arrival_airport": f.get("estArrivalAirport"),
            "callsign": (f.get("callsign") or "").strip() or None,
            "est_departure_airport_horiz_distance": f.get("estDepartureAirportHorizDistance"),
            "est_departure_airport_vert_distance": f.get("estDepartureAirportVertDistance"),
            "est_arrival_airport_horiz_distance": f.get("estArrivalAirportHorizDistance"),
            "est_arrival_airport_vert_distance": f.get("estArrivalAirportVertDistance"),
            "departure_airport_candidates_count": f.get("departureAirportCandidatesCount"),
            "arrival_airport_candidates_count": f.get("arrivalAirportCandidatesCount"),
            "flight_direction": f.get("flight_direction"),
            "queried_airport": f.get("queried_airport"),
            "ingested_at": now,
        })
    cursor = conn.cursor()
    cursor.executemany(INSERT_RAW_SQL, rows)
    return len(rows)


def load_seed_data(conn):
    cursor = conn.cursor()

    # Load airports
    for row in AIRPORTS_SEED:
        cursor.execute(
            f"INSERT INTO {BACKFILL_SCHEMA}.seed_airports VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            row,
        )
    logger.info("Loaded %d airports into seed_airports.", len(AIRPORTS_SEED))

    # Load airlines
    for row in AIRLINES_SEED:
        cursor.execute(
            f"INSERT INTO {BACKFILL_SCHEMA}.seed_airlines VALUES (%s,%s,%s,%s,%s)",
            row,
        )
    logger.info("Loaded %d airlines into seed_airlines.", len(AIRLINES_SEED))


def build_gold_tables(conn):
    cursor = conn.cursor()
    gold_tables = [
        ("gld_daily_airport_traffic", GOLD_DAILY_AIRPORT_TRAFFIC),
        ("gld_route_analysis", GOLD_ROUTE_ANALYSIS),
        ("gld_hourly_flight_activity", GOLD_HOURLY_FLIGHT_ACTIVITY),
        ("gld_airline_traffic", GOLD_AIRLINE_TRAFFIC),
    ]
    for name, sql in gold_tables:
        logger.info("Building %s.%s ...", BACKFILL_SCHEMA, name)
        cursor.execute(sql)
        # Log row count
        cursor.execute(f"SELECT COUNT(*) FROM {BACKFILL_SCHEMA}.{name}")
        count = cursor.fetchone()[0]
        logger.info("  → %d rows", count)


# ── Main ───────────────────────────────────────────────────────────────
def main():
    token = get_access_token()
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Step 1: Set up schema and tables
    logger.info("Setting up BACKFILL schema and tables...")
    for stmt in SETUP_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)

    # Step 2: Load seed data
    logger.info("Loading seed data...")
    load_seed_data(conn)

    # Step 3: Fetch and load 7 days of flight data
    now_utc = datetime.now(tz=timezone.utc)
    total_loaded = 0

    for days_ago in range(1, DAYS_TO_BACKFILL + 1):
        day = now_utc - timedelta(days=days_ago)
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
        begin = int(day_start.timestamp())
        end = int(day_end.timestamp())

        day_label = day_start.strftime("%Y-%m-%d")
        logger.info("── Fetching %s ──", day_label)

        day_flights = []
        for airport in AIRPORTS:
            for direction in ("arrival", "departure"):
                flights = fetch_flights(token, airport, begin, end, direction)
                day_flights.extend(flights)
                logger.info(
                    "  %s %s: %d flights", airport, direction, len(flights)
                )
                time.sleep(API_PAUSE_SECONDS)

        loaded = load_raw_flights(conn, day_flights)
        total_loaded += loaded
        logger.info("  Loaded %d flights for %s", loaded, day_label)

    logger.info("━━ Raw ingestion complete: %d total flights ━━", total_loaded)

    # Step 4: Build gold tables
    logger.info("Building Gold tables...")
    build_gold_tables(conn)

    conn.close()
    logger.info("━━ Backfill complete! Tables in %s.%s ━━",
                os.environ["SNOWFLAKE_DATABASE"], BACKFILL_SCHEMA)


if __name__ == "__main__":
    main()
