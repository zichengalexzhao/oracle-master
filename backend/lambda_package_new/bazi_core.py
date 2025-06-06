import datetime
import pytz
import ephem
from timezonefinder import TimezoneFinder
import logging

# Constants for BaZi calculation
GAN = ["Jia", "Yi", "Bing", "Ding", "Wu", "Ji", "Geng", "Xin", "Ren", "Gui"]
ZHI = ["Zi", "Chou", "Yin", "Mao", "Chen", "Si", "Wu", "Wei", "Shen", "You", "Xu", "Hai"]
ZHI_HOUR_MAPPING = {
    "Zi": (23, 1), "Chou": (1, 3), "Yin": (3, 5), "Mao": (5, 7), "Chen": (7, 9),
    "Si": (9, 11), "Wu": (11, 13), "Wei": (13, 15), "Shen": (15, 17), "You": (17, 19),
    "Xu": (19, 21), "Hai": (21, 23)
}
JD_ORIGIN = 2427879.5

tf = TimezoneFinder()

def to_julian(dt):
    """Convert datetime to Julian date."""
    unix_time = dt.timestamp()
    return unix_time / 86400.0 + 2440587.5

def get_timezone(city, longitude, latitude=None):
    """Get timezone for a given city and longitude."""
    if latitude is not None:
        timezone_str = tf.timezone_at(lat=latitude, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude} and latitude {latitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    try:
        # Note: geopy.geocoders.Nominatim is not used here to avoid network calls
        # Assume longitude-based timezone approximation
        timezone_str = tf.timezone_at(lat=0, lng=longitude)
        if timezone_str is None:
            return pytz.UTC, f"Warning: Cannot determine timezone for city {city} with longitude {longitude}. Using UTC as default."
        return pytz.timezone(timezone_str), None
    except Exception as e:
        return pytz.UTC, f"Warning: Failed to determine timezone for city {city} due to {str(e)}. Using UTC as default."

def calc_solar_term(jd):
    """Calculate the solar term based on Julian date."""
    sun = ephem.Sun()
    observer = ephem.Observer()
    observer.date = ephem.julian_date(jd)
    sun.compute(observer)
    hlon = sun.hlon * 180 / 3.14159
    if 315 <= hlon < 345:
        return "Yin"
    return "Mao"

def get_four_pillars(birth_datetime, location):
    """
    Calculate the Four Pillars (Year, Month, Day, Hour) based on birth date and location.
    
    Args:
        birth_datetime (str): Birth date and time in ISO format (e.g., "1990-03-12T15:00:00Z").
        location (str or dict): Location as a string (e.g., "Tokyo, Japan") or dict (e.g., {"city": "Tokyo", "longitude": 139.7}).
    
    Returns:
        dict: Four Pillars with their Heavenly Stems and Earthly Branches.
    """
    try:
        # Parse birth datetime
        if isinstance(birth_datetime, str):
            dt = datetime.datetime.fromisoformat(birth_datetime.replace("Z", "+00:00"))
        else:
            dt = birth_datetime

        # Determine location and timezone
        if isinstance(location, dict):
            city = location.get("city", "Unknown")
            longitude = location.get("longitude", 0)
            latitude = location.get("latitude", None)
        else:
            city = location
            longitude = 0  # Simplified for in-browser compatibility
            latitude = None

        tz, warning = get_timezone(city, longitude, latitude)
        if warning:
            logging.warning(warning)

        # Localize datetime to the correct timezone
        dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)

        # Calculate Julian date
        jd = to_julian(dt)

        # Year Pillar
        year = dt.year
        year_stem_idx = (year - 4) % 10
        year_branch_idx = (year - 4) % 12
        year_pillar = {"stem": GAN[year_stem_idx], "branch": ZHI[year_branch_idx]}

        # Month Pillar
        month_branch = calc_solar_term(jd)
        month_branch_idx = ZHI.index(month_branch)
        month_stem_base = (year_stem_idx * 2 + month_branch_idx) % 10
        month_pillar = {"stem": GAN[month_stem_base], "branch": month_branch}

        # Day Pillar
        day_number = int(jd - JD_ORIGIN + 0.5)
        day_stem_idx = day_number % 10
        day_branch_idx = day_number % 12
        day_pillar = {"stem": GAN[day_stem_idx], "branch": ZHI[day_branch_idx]}

        # Hour Pillar
        hour = dt.hour
        for zhi, (start, end) in ZHI_HOUR_MAPPING.items():
            if start <= hour < end or (start > end and (hour >= start or hour < end)):
                hour_branch = zhi
                break
        else:
            hour_branch = "Zi" if hour == 23 else "Unknown"
        hour_branch_idx = ZHI.index(hour_branch)
        hour_stem_base = (day_stem_idx * 2 + hour_branch_idx) % 10
        hour_pillar = {"stem": GAN[hour_stem_base], "branch": hour_branch}

        result = {
            "year_pillar": year_pillar,
            "month_pillar": month_pillar,
            "day_pillar": day_pillar,
            "hour_pillar": hour_pillar,
            "timestampTST": dt.isoformat(),
            "warning": warning
        }
        return result
    except Exception as e:
        logging.error(f"Error in get_four_pillars: {str(e)}")
        raise

def get_luck_pillars(four_pillars, gender='unknown'):
    """
    Calculate Luck Pillars (Da Yun) based on the Four Pillars.
    
    Args:
        four_pillars (dict): The Four Pillars result from get_four_pillars.
        gender (str): Gender of the person ('male', 'female', or 'unknown').
    
    Returns:
        list: List of Luck Pillars with their Heavenly Stems and Earthly Branches.
    """
    try:
        year_stem = four_pillars['year_pillar']['stem']
        month_branch = four_pillars['month_pillar']['branch']
        year_stem_idx = GAN.index(year_stem)
        month_branch_idx = ZHI.index(month_branch)

        # Determine direction of luck pillars (forward or backward)
        if gender == 'male':
            direction = 1 if year_stem_idx % 2 == 0 else -1
        else:  # Female or unknown
            direction = -1 if year_stem_idx % 2 == 0 else 1

        luck_pillars = []
        for i in range(1, 9):  # Calculate 8 luck pillars (10 years each)
            new_month_branch_idx = (month_branch_idx + i * direction) % 12
            if new_month_branch_idx < 0:
                new_month_branch_idx += 12
            new_month_branch = ZHI[new_month_branch_idx]
            new_month_stem_base = (year_stem_idx * 2 + new_month_branch_idx) % 10
            new_month_stem = GAN[new_month_stem_base]
            luck_pillars.append({
                "start_age": i * 10,
                "stem": new_month_stem,
                "branch": new_month_branch
            })

        return luck_pillars
    except Exception as e:
        logging.error(f"Error in get_luck_pillars: {str(e)}")
        raise