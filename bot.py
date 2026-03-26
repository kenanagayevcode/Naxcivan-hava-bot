import os
import sqlite3
import logging
import random
import threading
import asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# ENV
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWM_API_KEY = os.getenv("OWM_API_KEY", "").strip()
PORT = int(os.getenv("PORT", "10000"))
TIMEZONE = os.getenv("BOT_TIMEZONE", "Asia/Baku").strip()
USE_HEALTH_SERVER = os.getenv("USE_HEALTH_SERVER", "1").strip() == "1"
DB_PATH = os.getenv("DB_PATH", "weather_bot.db").strip()
HEALTH_TEXT = "Naxcivan Weather Bot is alive"

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("naxcivan_weather_bot")
logging.getLogger("httpx").setLevel(logging.WARNING)

# =========================
# REGIONS
# =========================
REGIONS: Dict[str, Dict[str, List[str]]] = {
    "Naxçıvan şəhəri": {
        "Şəhər": ["Naxçıvan (şəhər)"],
        "Qəsəbələr": ["Əliabad (qəsəbə)"],
        "Kəndlər": ["Başbaşı", "Bulqan", "Haciniyyət", "Qaraxanbəyli", "Qaraçuq", "Tumbul"]
    },
    "Babək rayonu": {
        "Şəhər": ["Babək (şəhər)"],
        "Qəsəbələr": ["Cəhri", "Nehrəm"],
        "Kəndlər": [
            "Alagözməzrə", "Araz", "Aşağı Buzqov", "Aşağı Uzunoba", "Badaşqan",
            "Çeşməbasar", "Didivar", "Gərməçataq", "Göynük", "Gülşənabad",
            "Güznüt", "Hacıvar", "Xalxal", "Xəlilli", "Kərimbəyli", "Kültəpə",
            "Qahab", "Qaraqala", "Məmmədrza Dizə", "Naxışnərgiz", "Nəcəfəlidizə",
            "Nəhəcir", "Nəzərabad", "Payız", "Sirab", "Şəkərabad", "Şıxmahmud",
            "Vayxır", "Yarımca", "Yuxarı Buzqov", "Yuxarı Uzunoba", "Zeynəddin"
        ]
    },
    "Culfa rayonu": {
        "Şəhər": ["Culfa (şəhər)"],
        "Qəsəbələr": [],
        "Kəndlər": [
            "Əbrəqunus", "Əlincə", "Ərəfsə", "Ərəzin", "Bənəniyar", "Boyəhməd",
            "Camaldın", "Dizə", "Gal", "Göydərə", "Göynük", "Gülüstan",
            "Xanəgah", "Xoşkeşin", "Kırna", "Qazançı", "Qızılca", "Ləkətağ",
            "Milax", "Saltax", "Şurud", "Teyvaz", "Yaycı"
        ]
    },
    "Kəngərli rayonu": {
        "Şəhər": [],
        "Qəsəbələr": ["Qıvraq"],
        "Kəndlər": ["Böyükdüz", "Çalxanqala", "Xok", "Xıncab", "Qabıllı", "Qarabağlar", "Şahtaxtı", "Təzəkənd", "Yeni Kərki", "Yurdçu"]
    },
    "Ordubad rayonu": {
        "Şəhər": ["Ordubad (şəhər)"],
        "Qəsəbələr": ["Parağaçay", "Şəhriyar"],
        "Kəndlər": [
            "Ağdərə", "Ağrı", "Anabad", "Anaqut", "Aşağı Əndəmic", "Aşağı Əylis",
            "Aza", "Azadkənd", "Baş Dizə", "Başkənd", "Behrud", "Biləv", "Bist",
            "Çənnəb", "Darkənd", "Dəstə", "Dırnıs", "Dizə", "Düylün", "Ələhi",
            "Gənzə", "Gilançay", "Xanağa", "Xurs", "Kələki", "Kələntər Dizə",
            "Kilit", "Kotam", "Məzrə", "Nəsirvaz", "Nürgüt", "Nüsnüs", "Parağa",
            "Pəzməri", "Qoşadizə", "Qoruqlar", "Sabirkənd", "Tivi", "Unus",
            "Üstüpü", "Vələver", "Vənənd", "Yuxarı Əndəmic", "Yuxarı Əylis"
        ]
    },
    "Sədərək rayonu": {
        "Şəhər": [],
        "Qəsəbələr": ["Heydərabad"],
        "Kəndlər": ["Dəmirçi", "Kərki", "Qaraağac", "Sədərək"]
    },
    "Şahbuz rayonu": {
        "Şəhər": ["Şahbuz (şəhər)"],
        "Qəsəbələr": ["Badamlı"],
        "Kəndlər": ["Ağbulaq", "Aşağı Qışlaq", "Ayrınc", "Biçənək", "Daylaqlı", "Gömür",
                     "Güney Qışlaq", "Keçili", "Kiçikoba", "Kolanı", "Kükü", "Külüs",
                     "Qızıl Qışlaq", "Mahmudoba", "Mərəlik", "Nursu", "Sələsüz", "Şada",
                     "Şahbuzkənd", "Türkeş", "Yuxarı Qışlaq"]
    },
    "Şərur rayonu": {
        "Şəhər": ["Şərur (şəhər)"],
        "Qəsəbələr": [],
        "Kəndlər": [
            "Axaməd", "Axura", "Alışar", "Arbatan", "Arpaçay", "Aşağı Aralıq", "Aşağı Daşarx",
            "Aşma", "Çərçiboğan", "Çomaxtur", "Dərvişlər", "Dəmirçi", "Dərəkənd", "Dizə",
            "Gümüşlü", "Günnüt", "Xələc", "Xanlıqlar", "İbadulla", "Kərimbəyli", "Kürkənd",
            "Mahmudkənd", "Maxta", "Muğanlı", "Oğlanqala", "Püsyan", "Qorçulu", "Sərxanlı",
            "Siyaqut", "Şəhriyar", "Tənənəm", "Tumaslı", "Vərməziyar", "Yengicə", "Yuxarı Aralıq",
            "Yuxarı Daşarx", "Zeyvə"
        ]
    }
}

# fallback koordinatlar
MANUAL_COORDS: Dict[str, Tuple[float, float]] = {
    "Naxçıvan (şəhər)": (39.2089, 45.4122),
    "Babək (şəhər)": (39.1500, 45.4489),
    "Culfa (şəhər)": (38.9539, 45.6296),
    "Ordubad (şəhər)": (38.9096, 46.0227),
    "Şahbuz (şəhər)": (39.4072, 45.5739),
    "Şərur (şəhər)": (39.5536, 44.9799),
    "Qıvraq": (39.3984, 45.1156),
    "Heydərabad": (39.7145, 44.8842),
    "Axura": (39.6129, 45.0355),
    "Arpaçay": (39.5600, 45.0100),
    "Püsyan": (39.5444, 44.9575),
    "Yengicə": (39.5184, 44.9400),
    "Zeyvə": (39.5748, 44.9520),
}

WEATHER_ICONS = {
    "clear": "☀️",
    "clouds": "☁️",
    "few clouds": "🌤",
    "scattered clouds": "🌤",
    "broken clouds": "☁️",
    "overcast clouds": "☁️",
    "rain": "🌧",
    "drizzle": "🌦",
    "shower rain": "🌧",
    "thunderstorm": "⛈",
    "snow": "❄️",
    "mist": "🌫",
    "fog": "🌫",
    "haze": "🌫",
}

MOTIVATION_QUOTES = {
    "hot": [
        "Bu gün isti olsa da, gücünü düzgün istifadə etsən gün sənin olacaq. Su iç, özünü qoru, ritmini itirmə.",
        "İsti hava səni yavaşlatmasın. Sakit, planlı və rahat hərəkət et, günün məhsuldar keçsin.",
        "Günəş güclüdür, amma sənin iradən daha güclü ola bilər. Özünü qoru və enerjini düzgün böl."
    ],
    "warm": [
        "Bu hava hərəkət etmək, işlərini yoluna qoymaq və özünü yaxşı hiss etmək üçün gözəl fürsətdir.",
        "Açıq hava ruhu da açır. Kiçik addımlar at, günün sonunda özündən razı qal.",
        "Bu gün balanslı və rahat keçə bilər. Tələsmə, amma dayanmadan davam et."
    ],
    "mild": [
        "Rahat hava rahat düşüncə yaradır. Sakit və dəqiq addımlarla işlərini tamamla.",
        "Bu gün nə çox ağır, nə də çox çətindir. Öz tempini qorusan hər şey yaxşı gedəcək.",
        "Yumşaq hava kimi sən də bu günü sakit və uğurlu keçirə bilərsən."
    ],
    "cool": [
        "Bir az sərin hava bəzən insanı daha ayıq və fokuslu edir. Bu günü dəyərləndir.",
        "Sərinlik varsa, ruhunu isti saxla. Kiçik uğurlar da böyük motivasiya yaradır.",
        "Hava sərin ola bilər, amma niyyətin isti qalmalıdır."
    ],
    "cold": [
        "Soyuq günlərdə də iradə isti qalmalıdır. Özünü qoru və planından ayrılma.",
        "Bu hava səbr və hazırlıq istəyir. Sakit qal, düzgün geyin və yoluna davam et.",
        "Çöldə hava sərtdirsə, daxilindəki gücü daha yaxşı hiss et."
    ]
}

PLACE_MAP: Dict[str, Dict[str, str]] = {}
GEO_CACHE: Dict[str, Tuple[float, float, str]] = {}

# =========================
# HELPERS
# =========================
def norm(s: str) -> str:
    return " ".join((s or "").strip().split())

def build_place_ids() -> None:
    idx = 1
    for region_name, data in REGIONS.items():
        all_places = data["Şəhər"] + data["Qəsəbələr"] + data["Kəndlər"]
        for place in all_places:
            PLACE_MAP[f"p{idx}"] = {"region": region_name, "place": place}
            idx += 1

def get_place_id(region: str, place: str) -> Optional[str]:
    for pid, item in PLACE_MAP.items():
        if item["region"] == region and item["place"] == place:
            return pid
    return None

def pick_icon(desc: str) -> str:
    d = (desc or "").lower()
    for key, emoji in WEATHER_ICONS.items():
        if key in d:
            return emoji
    return "🌡"

def pick_motivation(temp: float) -> str:
    if temp >= 35:
        return random.choice(MOTIVATION_QUOTES["hot"])
    elif temp >= 25:
        return random.choice(MOTIVATION_QUOTES["warm"])
    elif temp >= 15:
        return random.choice(MOTIVATION_QUOTES["mild"])
    elif temp >= 5:
        return random.choice(MOTIVATION_QUOTES["cool"])
    return random.choice(MOTIVATION_QUOTES["cold"])

def make_warning(desc: str, wind_speed: float, temp: float) -> str:
    d = (desc or "").lower()
    warnings = []

    if "rain" in d or "drizzle" in d:
        warnings.append("🌧 Yağış ehtimalı / yağış var. Çölə çıxırsansa çətir götürmək yaxşı olar.")
    if "snow" in d:
        warnings.append("❄️ Qar şəraiti var. Yollarda ehtiyatlı ol, isti geyin.")
    if wind_speed is not None and wind_speed >= 10:
        warnings.append(f"🌬 Güclü külək müşahidə olunur ({wind_speed} m/s). Açıq sahədə ehtiyatlı ol.")
    if temp is not None and temp >= 35:
        warnings.append("🔥 Temperatur yüksəkdir. Gün altında uzun müddət qalma və bol su iç.")
    if temp is not None and temp <= 0:
        warnings.append("🧊 Temperatur çox aşağıdır. Sürüşkənlik və soyuqlama riskinə diqqət et.")

    if not warnings:
        return "✅ Hazırda xüsusi hava xəbərdarlığı görünmür."

    return "\n".join(warnings)

# =========================
# DB
# =========================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            last_place_id TEXT,
            favorite_place_id TEXT,
            daily_enabled INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocache (
            cache_key TEXT PRIMARY KEY,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            resolved_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

def upsert_user(chat_id: int, username: str = "", first_name: str = "", last_name: str = "") -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (chat_id, username, first_name, last_name, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(chat_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            updated_at=CURRENT_TIMESTAMP
    """, (chat_id, username, first_name, last_name))
    conn.commit()
    conn.close()

def get_user(chat_id: int):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row

def update_user_place(chat_id: int, place_id: str) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET last_place_id=?, updated_at=CURRENT_TIMESTAMP
        WHERE chat_id=?
    """, (place_id, chat_id))
    conn.commit()
    conn.close()

def set_favorite(chat_id: int, place_id: str, daily_enabled: int = 1) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET favorite_place_id=?, daily_enabled=?, updated_at=CURRENT_TIMESTAMP
        WHERE chat_id=?
    """, (place_id, daily_enabled, chat_id))
    conn.commit()
    conn.close()

def disable_daily(chat_id: int) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET daily_enabled=0, updated_at=CURRENT_TIMESTAMP
        WHERE chat_id=?
    """, (chat_id,))
    conn.commit()
    conn.close()

def save_geocache(cache_key: str, lat: float, lon: float, resolved_name: str) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO geocache (cache_key, lat, lon, resolved_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            lat=excluded.lat,
            lon=excluded.lon,
            resolved_name=excluded.resolved_name
    """, (cache_key, lat, lon, resolved_name))
    conn.commit()
    conn.close()

def load_geocache(cache_key: str):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT lat, lon, resolved_name FROM geocache WHERE cache_key=?", (cache_key,))
    row = cur.fetchone()
    conn.close()
    if row:
        return float(row["lat"]), float(row["lon"]), str(row["resolved_name"] or "")
    return None

def get_daily_users():
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM users
        WHERE daily_enabled=1
          AND favorite_place_id IS NOT NULL
          AND favorite_place_id <> ''
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# =========================
# NETWORK
# =========================
def sync_get_json(url: str, params: dict, timeout: int = 20):
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()

async def async_get_json(url: str, params: dict, timeout: int = 20):
    return await asyncio.to_thread(sync_get_json, url, params, timeout)

async def geocode_place(place: str, region: Optional[str] = None) -> Optional[Tuple[float, float, str]]:
    place = norm(place)
    region = norm(region or "")
    cache_key = f"{place}|{region}"

    if cache_key in GEO_CACHE:
        return GEO_CACHE[cache_key]

    cached = load_geocache(cache_key)
    if cached:
        GEO_CACHE[cache_key] = cached
        return cached

    if place in MANUAL_COORDS:
        lat, lon = MANUAL_COORDS[place]
        result = (lat, lon, place)
        GEO_CACHE[cache_key] = result
        save_geocache(cache_key, lat, lon, place)
        return result

    queries = []
    if region:
        queries.append(f"{place}, {region}, Naxçıvan, Azerbaijan")
        queries.append(f"{place}, {region}, Azerbaijan")
    queries.append(f"{place}, Naxçıvan, Azerbaijan")
    queries.append(f"{place}, Azerbaijan")
    queries.append(place)

    url = "http://api.openweathermap.org/geo/1.0/direct"

    for q in queries:
        try:
            results = await async_get_json(url, {
                "q": q,
                "limit": 5,
                "appid": OWM_API_KEY
            })

            if not isinstance(results, list) or not results:
                continue

            best = None
            wanted_region = region.lower().replace(" rayonu", "").replace(" şəhəri", "").strip()

            for item in results:
                name = norm(item.get("name", ""))
                state = norm(item.get("state", ""))
                country = norm(item.get("country", ""))

                lat = item.get("lat")
                lon = item.get("lon")
                if lat is None or lon is None:
                    continue

                score = 0
                if country.upper() == "AZ":
                    score += 5
                if place.lower() == name.lower():
                    score += 7
                elif place.lower() in name.lower():
                    score += 4
                if wanted_region and wanted_region in state.lower():
                    score += 5
                if "nakhchivan" in state.lower() or "naxçıvan" in state.lower():
                    score += 4

                candidate = {
                    "score": score,
                    "lat": float(lat),
                    "lon": float(lon),
                    "resolved_name": name or place
                }

                if best is None or candidate["score"] > best["score"]:
                    best = candidate

            if best:
                result = (best["lat"], best["lon"], best["resolved_name"])
                GEO_CACHE[cache_key] = result
                save_geocache(cache_key, result[0], result[1], result[2])
                return result

        except Exception as e:
            logger.warning("Geocode error for %s / %s: %s", place, region, e)

    return None

async def fetch_current_weather(lat: float, lon: float):
    try:
        return await async_get_json(
            "https://api.openweathermap.org/data/2.5/weather",
            {
                "lat": lat,
                "lon": lon,
                "appid": OWM_API_KEY,
                "units": "metric",
                "lang": "az"
            }
        )
    except Exception as e:
        logger.warning("Current weather fetch error: %s", e)
        return None

async def fetch_forecast(lat: float, lon: float):
    try:
        return await async_get_json(
            "https://api.openweathermap.org/data/2.5/forecast",
            {
                "lat": lat,
                "lon": lon,
                "appid": OWM_API_KEY,
                "units": "metric",
                "lang": "az"
            }
        )
    except Exception as e:
        logger.warning("Forecast fetch error: %s", e)
        return None

def summarize_forecast(forecast_data: Optional[dict]) -> str:
    if not forecast_data or "list" not in forecast_data:
        return "🕒 Yaxın saatlar üzrə əlavə proqnoz hazırda göstərilə bilmədi."

    items = forecast_data.get("list", [])[:4]
    if not items:
        return "🕒 Yaxın saatlar üzrə proqnoz tapılmadı."

    lines = []
    for item in items:
        dt_txt = str(item.get("dt_txt", ""))[11:16]
        main = item.get("main", {}) or {}
        weather = (item.get("weather", [{}]) or [{}])[0]
        temp = main.get("temp", "?")
        desc = str(weather.get("description", ""))
        icon = pick_icon(desc)
        lines.append(f"• {dt_txt} — {icon} {temp}°C, {desc}")

    return "🕒 *Yaxın saatlar üzrə proqnoz*\n" + "\n".join(lines)

def build_weather_text(place: str, region: str, current: dict, forecast: Optional[dict], is_daily: bool = False) -> str:
    main = current.get("main", {}) or {}
    wind = current.get("wind", {}) or {}
    clouds = current.get("clouds", {}) or {}
    weather_item = (current.get("weather", [{}]) or [{}])[0]

    temp = main.get("temp")
    feels = main.get("feels_like")
    temp_min = main.get("temp_min")
    temp_max = main.get("temp_max")
    humidity = main.get("humidity")
    pressure = main.get("pressure")
    wind_speed = wind.get("speed", 0)
    cloudiness = clouds.get("all")
    desc = str(weather_item.get("description", "Məlumat yoxdur")).lower()
    icon = pick_icon(desc)

    motivation = pick_motivation(float(temp)) if temp is not None else "Günün uğurlu keçsin."
    warning = make_warning(desc, float(wind_speed or 0), float(temp) if temp is not None else None)
    forecast_text = summarize_forecast(forecast)

    top = "⏰ *Saat 09:00 gündəlik hava məlumatı*\n\n" if is_daily else ""

    return (
        f"{top}"
        f"📍 *{place}*\n"
        f"🗺 *Bölgə:* {region}\n\n"
        f"{icon} *Cari vəziyyət:* {desc}\n"
        f"🌡 *Temperatur:* {temp}°C\n"
        f"🤗 *Hiss edilən:* {feels}°C\n"
        f"📉 *Minimum:* {temp_min}°C\n"
        f"📈 *Maksimum:* {temp_max}°C\n"
        f"💧 *Rütubət:* {humidity}%\n"
        f"🌬 *Külək:* {wind_speed} m/s\n"
        f"☁️ *Buludluluq:* {cloudiness}%\n"
        f"🧭 *Təzyiq:* {pressure} hPa\n\n"
        f"{forecast_text}\n\n"
        f"🚨 *Xəbərdarlıq*\n{warning}\n\n"
        f"✨ *Motivasiya*\n{motivation}"
    )

# =========================
# KEYBOARDS
# =========================
def build_regions_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for region_name, data in REGIONS.items():
        for city in data["Şəhər"]:
            if pid:
                keyboard.append([InlineKeyboardButton(f"🏙 {city}", callback_data=f"weather|{pid}")])
    keyboard.append([InlineKeyboardButton("⭐ Sevimli məkanım", callback_data="myfav")])
    return InlineKeyboardMarkup(keyboard)

def build_places_keyboard(region: str) -> InlineKeyboardMarkup:
    all_places = REGIONS[region]["Qəsəbələr"] + REGIONS[region]["Kəndlər"]
    keyboard = []
    row = []
    for i, place in enumerate(all_places, start=1):
        pid = get_place_id(region, place)
        if not pid:
            continue
        row.append(InlineKeyboardButton(place, callback_data=f"weather|{pid}"))
        if i % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("⬅️ Şəhərlər", callback_data="regions")])
    keyboard.append([InlineKeyboardButton("⭐ Sevimli məkanım", callback_data="myfav")])
    return InlineKeyboardMarkup(keyboard)

def build_weather_actions(place_id: str) -> InlineKeyboardMarkup:
    item = PLACE_MAP.get(place_id, {})
    region = item.get("region", "")
    keyboard = [
        [
            InlineKeyboardButton("⭐ Sevimli et", callback_data=f"fav|{place_id}"),
            InlineKeyboardButton("🔔 09:00 aktiv et", callback_data=f"dailyon|{place_id}")
        ],
        [
            InlineKeyboardButton("🔕 09:00 söndür", callback_data="dailyoff")
        ]
    ]
    if region:
        keyboard.append([InlineKeyboardButton("📍 Bu rayonun kəndləri", callback_data=f"regionplaces|{region}")])
    keyboard.append([InlineKeyboardButton("🏙 Şəhərlər", callback_data="regions")])
    return InlineKeyboardMarkup(keyboard)

# =========================
# JOB MANAGEMENT
# =========================
def remove_user_jobs(application: Application, chat_id: int) -> None:
    job_name = f"daily_{chat_id}"
    jobs = application.job_queue.get_jobs_by_name(job_name)
    for job in jobs:
        job.schedule_removal()

def schedule_daily_job(application: Application, chat_id: int, place_id: str) -> None:
    remove_user_jobs(application, chat_id)
    application.job_queue.run_daily(
        daily_weather_job,
        time=dtime(hour=9, minute=0, tzinfo=ZoneInfo(TIMEZONE)),
        data={"chat_id": chat_id, "place_id": place_id},
        name=f"daily_{chat_id}",
        chat_id=chat_id
    )

def restore_daily_jobs(application: Application) -> None:
    rows = get_daily_users()
    for row in rows:
        try:
            schedule_daily_job(application, int(row["chat_id"]), str(row["favorite_place_id"]))
        except Exception as e:
            logger.exception("Restore daily job error for %s: %s", row["chat_id"], e)

# =========================
# HEALTH SERVER
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(HEALTH_TEXT.encode("utf-8"))

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        logger.info("Health server started at port %s", PORT)
        server.serve_forever()
    except Exception as e:
        logger.exception("Health server error: %s", e)

# =========================
# CORE
# =========================
async def send_weather(update: Update, context: ContextTypes.DEFAULT_TYPE, place_id: str, daily: bool = False, prefix: str = "") -> None:
    item = PLACE_MAP.get(place_id)
    if not item:
        await update.effective_message.reply_text("❌ Məkan tapılmadı.")
        return

    place = item["place"]
    region = item["region"]

    user = update.effective_user
    chat = update.effective_chat
    if user and chat:
        upsert_user(chat.id, user.username or "", user.first_name or "", user.last_name or "")
        update_user_place(chat.id, place_id)

    geo = await geocode_place(place, region)
    if not geo:
        await update.effective_message.reply_text(
            f"❌ *{place}* üçün koordinat tapılmadı.\nRayon: *{region}*",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    lat, lon, _ = geo
    current = await fetch_current_weather(lat, lon)
    forecast = await fetch_forecast(lat, lon)

    if not current:
        await update.effective_message.reply_text("⚠️ Hava məlumatı hazırda gətirilə bilmədi.")
        return

    text = prefix + build_weather_text(place, region, current, forecast, is_daily=daily)

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_weather_actions(place_id)
    )

async def daily_weather_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = int(data.get("chat_id"))
    place_id = str(data.get("place_id"))

    item = PLACE_MAP.get(place_id)
    if not item:
        return

    place = item["place"]
    region = item["region"]

    geo = await geocode_place(place, region)
    if not geo:
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ {place} üçün koordinat tapılmadı.")
        return

    lat, lon, _ = geo
    current = await fetch_current_weather(lat, lon)
    forecast = await fetch_forecast(lat, lon)

    if not current:
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ {place} üçün hava məlumatı gətirilə bilmədi.")
        return

    text = build_weather_text(place, region, current, forecast, is_daily=True)
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_weather_actions(place_id)
    )

# =========================
# COMMANDS
# =========================
async def post_init(app: Application) -> None:
    commands = [
        BotCommand("start", "Botu başlat"),
        BotCommand("menu", "Əsas menyu"),
        BotCommand("help", "Kömək"),
        BotCommand("fav", "Sevimli məkan"),
        BotCommand("dailyon", "09:00 bildirişi aktiv et"),
        BotCommand("dailyoff", "09:00 bildirişi söndür"),
    ]
    await app.bot.set_my_commands(commands)
    restore_daily_jobs(app)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if user and chat:
        upsert_user(chat.id, user.username or "", user.first_name or "", user.last_name or "")

    text = (
        "✨ *Salam, Naxçıvan Hava Botuna xoş gəlmisən!*\n\n"
        "Bu bot ilə:\n"
        "• şəhər, kənd və qəsəbə üzrə hava məlumatı ala bilərsən\n"
        "• son seçdiyin və sevimli etdiyin məkanı yadda saxlaya bilərsən\n"
        "• hər gün *09:00*-da avtomatik hava məlumatı ala bilərsən\n"
        "• yağış, qar, güclü külək kimi hallarda xəbərdarlıq görə bilərsən\n\n"
        "Aşağıdan şəhər seç."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=build_regions_keyboard())

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🏙 Şəhəri seç:", reply_markup=build_regions_keyboard())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "*İstifadə qaydası*\n\n"
        "1. Şəhər seç\n"
        "2. İstəsən həmin rayonun kənd və qəsəbələrini aç\n"
        "3. `⭐ Sevimli et` ilə məkanı yadda saxla\n"
        "4. `🔔 09:00 aktiv et` ilə hər gün avtomatik hava al\n\n"
        "*Komandalar*\n"
        "/start\n/menu\n/help\n/fav\n/dailyon\n/dailyoff"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def fav_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    row = get_user(update.effective_chat.id)
    if not row or not row["favorite_place_id"]:
        await update.message.reply_text("⭐ Hələ sevimli məkan seçilməyib.")
        return

    await send_weather(update, context, str(row["favorite_place_id"]), prefix="⭐ *Sevimli məkan üzrə hava məlumatı*\n\n")

async def dailyon_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    row = get_user(chat_id)
    if not row:
        await update.message.reply_text("⚠️ Əvvəl /start yaz və bir məkan seç.")
        return

    place_id = row["favorite_place_id"] or row["last_place_id"]
    if not place_id:
        await update.message.reply_text("⚠️ Əvvəlcə bir məkan seç.")
        return

    place_id = str(place_id)
    set_favorite(chat_id, place_id, daily_enabled=1)
    schedule_daily_job(context.application, chat_id, place_id)

    item = PLACE_MAP.get(place_id, {})
    await update.message.reply_text(
        f"🔔 *{item.get('place', 'Məkan')}* üçün hər gün saat 09:00 bildirişi aktiv edildi.",
        parse_mode=ParseMode.MARKDOWN
    )

async def dailyoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    disable_daily(chat_id)
    remove_user_jobs(context.application, chat_id)
    await update.message.reply_text("🔕 Saat 09:00 gündəlik bildiriş söndürüldü.")

# =========================
# TEXT SEARCH
# =========================
def find_place_id_by_text(text: str) -> Optional[str]:
    q = norm(text).lower()
    if not q:
        return None

    for pid, item in PLACE_MAP.items():
        if norm(item["place"]).lower() == q:
            return pid

    for pid, item in PLACE_MAP.items():
        if q in norm(item["place"]).lower():
            return pid

    for region_name, data in REGIONS.items():
        if norm(region_name).lower() == q and data["Şəhər"]:
            return get_place_id(region_name, data["Şəhər"][0])

    return None

async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pid = find_place_id_by_text(update.message.text or "")
    if not pid:
        await update.message.reply_text("❌ Məkan tanınmadı. Aşağıdan seç:", reply_markup=build_regions_keyboard())
        return
    await send_weather(update, context, pid)

# =========================
# CALLBACKS
# =========================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data == "regions":
        await query.message.reply_text("🏙 Şəhəri seç:", reply_markup=build_regions_keyboard())
        return

    if data == "myfav":
        row = get_user(update.effective_chat.id)
        if not row or not row["favorite_place_id"]:
            await query.message.reply_text("⭐ Hələ sevimli məkan seçilməyib.")
            return

        fake_update = Update(update.update_id, message=query.message)
        await send_weather(fake_update, context, str(row["favorite_place_id"]), prefix="⭐ *Sevimli məkan üzrə hava məlumatı*\n\n")
        return

    if data.startswith("weather|"):
        place_id = data.split("|", 1)[1]

        # istifadəçi yeni məkan seçəndə əvvəlki gündəlik job bağlanır, yalnız lazım olsa sonra yenidən qurulur
        row = get_user(update.effective_chat.id)
        if row and row["daily_enabled"] == 1 and row["favorite_place_id"]:
            old_fav = str(row["favorite_place_id"])
            if old_fav != place_id:
                remove_user_jobs(context.application, update.effective_chat.id)
                disable_daily(update.effective_chat.id)

        fake_update = Update(update.update_id, message=query.message)
        await send_weather(fake_update, context, place_id)

        item = PLACE_MAP.get(place_id)
        if item and item["place"] in REGIONS[item["region"]]["Şəhər"]:
            await query.message.reply_text(
                f"📍 *{item['place']}* üzrə kənd və qəsəbələr:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=build_places_keyboard(item["region"])
            )
        return

    if data.startswith("regionplaces|"):
        region = data.split("|", 1)[1]
        if region in REGIONS:
            await query.message.reply_text(
                f"📍 *{region}* üzrə kənd və qəsəbələr:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=build_places_keyboard(region)
            )
        else:
            await query.message.reply_text("⚠️ Rayon tapılmadı.")
        return

    if data.startswith("fav|"):
        place_id = data.split("|", 1)[1]
        set_favorite(update.effective_chat.id, place_id, daily_enabled=1)
        schedule_daily_job(context.application, update.effective_chat.id, place_id)

        item = PLACE_MAP.get(place_id, {})
        await query.message.reply_text(
            f"⭐ *{item.get('place', 'Məkan')}* sevimli olaraq yadda saxlanıldı.\n"
            f"🔔 Hər gün saat 09:00 bildirişi də aktiv edildi.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data.startswith("dailyon|"):
        place_id = data.split("|", 1)[1]
        set_favorite(update.effective_chat.id, place_id, daily_enabled=1)
        schedule_daily_job(context.application, update.effective_chat.id, place_id)

        item = PLACE_MAP.get(place_id, {})
        await query.message.reply_text(
            f"🔔 *{item.get('place', 'Məkan')}* üçün gündəlik 09:00 bildirişi aktiv edildi.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "dailyoff":
        disable_daily(update.effective_chat.id)
        remove_user_jobs(context.application, update.effective_chat.id)
        await query.message.reply_text("🔕 Saat 09:00 bildirişi söndürüldü.")
        return

    await query.message.reply_text("⚠️ Naməlum əməliyyat.")

# =========================
# ERROR
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("⚠️ Gözlənilməz xəta baş verdi. Zəhmət olmasa yenidən yoxla.")
    except Exception:
        pass

# =========================
# MAIN
# =========================
def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN tapılmadı.")
    if not OWM_API_KEY:
        raise RuntimeError("OWM_API_KEY tapılmadı.")

    build_place_ids()
    init_db()

    if USE_HEALTH_SERVER:
        threading.Thread(target=run_health_server, daemon=True).start()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("fav", fav_command))
    app.add_handler(CommandHandler("dailyon", dailyon_command))
    app.add_handler(CommandHandler("dailyoff", dailyoff_command))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_error_handler(error_handler)

    logger.info("Bot started successfully.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
