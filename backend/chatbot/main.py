from functions_framework import http
import json
import re
from google.cloud import aiplatform
from google.cloud.aiplatform.gapic.schema import predict
import pytz
from datetime import datetime, timedelta
import ephem
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import ReadTimeout

# 四柱和五行相关常量
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

# 初始化 geopy 和 timezonefinder
geolocator = Nominatim(user_agent="oracle_master", timeout=15)
tf = TimezoneFinder()

# 产品推荐数据
PRODUCTS = [
    {
        "name": "Golden Pixiu Statue",
        "element": "Metal",
        "purpose": "Attract wealth and stabilize finances",
        "description": "A consecrated Feng Shui item to draw prosperity. Place facing the main door.",
        "price": 79.99,
        "link": "https://oraclemaster.shop/pixiu-statue"
    },
    {
        "name": "Jade Success Amulet",
        "element": "Wood",
        "purpose": "Enhance career and guidance",
        "description": "A blessed jade amulet to attract mentors. Wear during meetings.",
        "price": 49.99,
        "link": "https://oraclemaster.shop/jade-amulet"
    }
]

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

def parse_query(query):
    if "career" in query.lower():
        return {"category": "Career", "year": extract_year(query), "product_element": "Wood"}
    elif "finance" in query.lower() or "wealth" in query.lower():
        return {"category": "Wealth", "year": extract_year(query), "product_element": "Metal"}
    return {"category": "General", "year": extract_year(query), "product_element": None}

def extract_year(query):
    match = re.search(r'\d{4}', query)
    return int(match.group()) if match else 2025

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(Exception)
)
def parse_birth_datetime(query, client, endpoint):
    prompt = f"""
    Extract the birth date and time from the following user input and convert it to ISO 8601 format (e.g., "1990-03-12T15:00:00Z").
    If the time is missing or approximate, use a reasonable default (e.g., 12:00 for "around noon").
    If the date is incomplete, return an error message.
    User input: {query}
    Respond with the ISO 8601 format string or an error message.
    """
    instance = predict.instance.TextGenerationPredictionInstance(content=prompt).to_value()
    response = client.predict(endpoint=endpoint, instances=[instance], parameters={"maxOutputTokens": 50, "timeout": 30})
    birth_datetime = response.predictions[0].content.strip()
    if "error" in birth_datetime.lower():
        raise ValueError(birth_datetime)
    return birth_datetime

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(Exception)
)
def parse_location(query, client, endpoint):
    prompt = f"""
    Extract the city name from the following user input.
    If a city is not specified, return "San Francisco" as the default.
    User input: {query}
    Respond with the city name only.
    """
    instance = predict.instance.TextGenerationPredictionInstance(content=prompt).to_value()
    response = client.predict(endpoint=endpoint, instances=[instance], parameters={"maxOutputTokens": 50, "timeout": 30})
    city = response.predictions[0].content.strip()
    return city

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(Exception)
)
def call_vertex_ai(data, query, client, endpoint):
    prompt = f"""
    You are an expert in Chinese Four Pillars of Destiny. Based on:
    Four Pillars: {json.dumps(data['fourPillars'])}
    Five Elements: {json.dumps(data['fiveElements'])}
    User Question: {query}
    Provide a response in English for American users:
    - **Reasoning**: Explain the fortune using simple terms (e.g., Mentor Star, Creativity Star).
    - **Advice**: Offer actionable, positive suggestions with specific timeframes.
    - **Product Recommendation**: Suggest a consecrated product matching the query's goal, include name, description, and link.
    - **Disclaimer**: State this is for guidance only, not professional advice.
    If the birth time is approximate (e.g., set to a default like 12:00), add a note:
    - **Note**: Since your exact birth time is unknown, we used 12:00 PM as a default. This may affect the accuracy of your Hour Pillar, but your Year, Month, and Day Pillars are still reliable for this reading.
    """
    instance = predict.instance.TextGenerationPredictionInstance(content=prompt).to_value()
    response = client.predict(endpoint=endpoint, instances=[instance], parameters={"maxOutputTokens": 500, "timeout": 60})
    return response.predictions[0].content

@http
def chatbot(request):
    try:
        req_body = request.get_json()
        query = req_body.get("query")
        # 初始化 Vertex AI 客户端（需要替换为实际的端点）
        client = aiplatform.gapic.PredictionServiceClient(client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"})
        endpoint = client.endpoint_path(project="disco-catcher-461916-c0", location="us-central1", endpoint="your-endpoint-id")
        # 解析出生时间和城市
        birth_datetime = parse_birth_datetime(query, client, endpoint)
        city = parse_location(query, client, endpoint)
        # 假设用户提供了经纬度（后续可以扩展）
        location = {"city": city, "longitude": 0.0}  # 默认经度，后续动态获取
        # 调用 CalculatePillars
        four_pillars_data = {
            "fourPillars": get_four_pillars(birth_datetime, location),
            "fiveElements": get_five_elements(get_four_pillars(birth_datetime, location))
        }
        parsed_query = parse_query(query)
        response = call_vertex_ai(four_pillars_data, query, client, endpoint)
        product = next((p for p in PRODUCTS if p["element"] == parsed_query["product_element"]), PRODUCTS[0])
        response += f"\n**Product Recommendation**: Try the {product['name']}, {product['description']} [Buy now: {product['link']}]."
        return json.dumps({"response": response})
    except Exception as e:
        return json.dumps({"error": str(e)}), 400