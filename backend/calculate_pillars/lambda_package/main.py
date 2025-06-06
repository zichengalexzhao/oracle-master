import json
import pytz
from datetime import datetime, timedelta
import ephem
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import ReadTimeout

JD_ORIGIN = 2427879.5
GAN_LIST = ["Jia", "Yi", "Bing", "Ding", "Wu", "Ji", "Geng", "Xin", "Ren", "Gui"]
ZHI_LIST = ["Zi", "Chou", "Yin", "Mao", "Chen", "Si", "Wu", "Wei", "Shen", "You", "Xu", "Hai"]
FIVE_ELEMENTS = {
    "Jia": "Wood", "Yi": "Wood", "Bing": "Fire", "Ding": "Fire",
    "Wu": "Earth", "Ji": "Earth", "Geng": "Metal", "Xin": "Metal",
    "Ren": "Water", "Gui": "Water",
    "Zi": "Water", "Chou": "Earth", "Yin": "Wood", "Mao": "Wood",
    "Chen": "Earth", "Si": "Fire", "Wu": "Fire", "Wei": "Earth",
    "Shen": "Metal", "You": "Metal", "Xu": "Earth", "Hai": "Water"
}
HIDDEN_STEMS = {
    "Zi": ["Gui"], "Chou": ["Ji", "Xin", "Gui"], "Yin": ["Jia", "Bing", "Wu"],
    "Mao": ["Yi"], "Chen": ["Wu", "Yi", "Gui"], "Si": ["Bing", "Geng", "Wu"],
    "Wu": ["Ding", "Ji"], "Wei": ["Ji", "Yi", "Ding"], "Shen": ["Geng", "Ren", "Wu"],
    "You": ["Xin"], "Xu": ["Wu", "Ding", "Xin"], "Hai": ["Ren", "Jia"]
}

geolocator = Nominatim(user_agent="oracle_master", timeout=15)
tf = TimezoneFinder()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ReadTimeout)
)
def get_timezone(city, longitude, latitude=None):
    if latitude is not None:
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude} and latitude {latitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    try:
        location = geolocator.geocode(city)
        if location is None:
            return pytz.UTC, f"Warning: Cannot find coordinates for city: {city}. Using UTC as default."
        timezone_str = tf.timezone_at(lat=location.latitude, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    except Exception as e:
        return pytz.UTC, f"Warning: Failed to determine timezone for city {city} due to {str(e)}. Using UTC as default."

def to_julian(dt):
    unix_time = dt.timestamp()
    return unix_time / 86400.0 + 2440587.5

def calc_month_branch(jd):
    sun = ephem.Sun()
    observer = ephem.Observer()
    observer.date = ephem.julian_date(jd)
    sun.compute(observer)
    hlon = sun.hlon * 180 / 3.14159
    branch = "Yin" if 315 <= hlon < 345 else "Mao"
    year_stem = "Geng"
    stem_idx = (GAN_LIST.index(year_stem) + 2) % 10
    return {"stem": GAN_LIST[stem_idx], "branch": branch}

def get_four_pillars(birth_datetime, location):
    dt = datetime.fromisoformat(birth_datetime.replace("Z", "+00:00"))
    city = location["city"]
    longitude = location["longitude"]
    latitude = location.get("latitude")
    tz, warning = get_timezone(city, longitude, latitude)
    local_dt = dt.astimezone(tz)
    tst_offset = longitude / 15 * 4 / 60
    tst_dt = local_dt + timedelta(hours=tst_offset)

    year = tst_dt.year
    year_pillar = {"stem": GAN_LIST[(year - 4) % 10], "branch": ZHI_LIST[(year - 4) % 12]}
    jd = to_julian(tst_dt)
    month_pillar = calc_month_branch(jd)
    day_idx = int(jd - JD_ORIGIN) % 60
    day_pillar = {"stem": GAN_LIST[day_idx % 10], "branch": ZHI_LIST[day_idx % 12]}
    hour_branch_idx = ((tst_dt.hour + 1) // 2) % 12
    hour_pillar = {
        "stem": GAN_LIST[(day_idx % 5 * 2 + hour_branch_idx) % 10],
        "branch": ZHI_LIST[hour_branch_idx]
    }
    return {
        "year_pillar": year_pillar,
        "month_pillar": month_pillar,
        "day_pillar": day_pillar,
        "hour_pillar": hour_pillar,
        "timestampTST": tst_dt.isoformat(),
        "warning": warning
    }

def get_five_elements(four_pillars):
    elements = {"Wood": 0, "Fire": 0, "Earth": 0, "Metal": 0, "Water": 0}
    for pillar in four_pillars.values():
        if isinstance(pillar, dict):
            elements[FIVE_ELEMENTS[pillar["stem"]]] += 1
            elements[FIVE_ELEMENTS[pillar["branch"]]] += 1
            for hidden_stem in HIDDEN_STEMS.get(pillar["branch"], []):
                elements[FIVE_ELEMENTS[hidden_stem]] += 0.3
    return elements

def lambda_handler(event, context):
    try:
        body = event.get("body", {})
        if isinstance(body, str):
            body = json.loads(body)
        birth_datetime = body.get("birth_datetime")
        location = body.get("location")
        if not birth_datetime or not location:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing birth_datetime or location"}),
                "headers": {"Access-Control-Allow-Origin": "*"}
            }
        four_pillars = get_four_pillars(birth_datetime, location)
        five_elements = get_five_elements(four_pillars)
        response = {
            "fourPillars": four_pillars,
            "fiveElements": five_elements
        }
        return {
            "statusCode": 200,
            "body": json.dumps(response),
            "headers": {"Access-Control-Allow-Origin": "*"}
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)}),
            "headers": {"Access-Control-Allow-Origin": "*"}
        }