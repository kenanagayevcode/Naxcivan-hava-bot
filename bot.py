import os
import sqlite3
import logging
import random
import threading
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
# ENV / CONFIG
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWM_API_KEY = os.getenv("OWM_API_KEY", "").strip()
PORT = int(os.getenv("PORT", "10000"))
TIMEZONE = os.getenv("BOT_TIMEZONE", "Asia/Baku").strip()
USE_HEALTH_SERVER = os.getenv("USE_HEALTH_SERVER", "1").strip() == "1"

DB_PATH = os.getenv("DB_PATH", "weather_bot.db").strip()

# Health check üçün Render/UptimeRobot
HEALTH_TEXT = "Naxcivan Weather Bot is alive"

# =========================
# LOGGING
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("naxcivan_weather_bot")

# =========================
# DATA
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

# Əl ilə əlavə etdiyimiz koordinatlar – geocoding ilişsə fallback işləyəcək
MANUAL_COORDS: Dict[str, Tuple[float, float]] = {
    "Naxçıvan (şəhər)": (39.2089, 45.4122),
    "Babək (şəhər)": (39.1500, 45.4489),
    "Culfa (şəhər)": (38.9539, 45.6296),
    "Ordubad (şəhər)": (38.9096, 46.0227),
    "Şahbuz (şəhər)": (39.4072, 45.5739),
    "Şərur (şəhər)": (39.5536, 44.9799),
    "Qıvraq": (39.3984, 45.1156),
    "Heydərabad": (39.7145, 44.8842),
    # Şərur üçün ayrıca vacib fallback-lər
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

WEATHER_QUOTES = {
    "hot": [
        "🔥 Bu hava güclü istidir. Su içməyi və gün altında çox qalmamağı unutma.",
        "☀️ Günəş sərtdir. Çöldəsənsə, yüngül geyim və kölgə yaxşı seçimdir.",
        "🥵 İsti hava var. Yorğunluq hissi ola bilər, özünü qoru."
    ],
    "warm": [
        "🌞 Hava xoş və istidir. Açıq hava planları üçün yaxşı gündür.",
        "🌼 Gəzinti və qısa açıq hava fəaliyyəti üçün münasib havadır.",
        "🍀 Rahat gündür, amma yenə də su içmək yaxşı olar."
    ],
    "mild": [
        "🌤 Nə isti, nə soyuq — rahat hava şəraitidir.",
        "😊 Gün ərzində rahat hərəkət etmək üçün uyğun havadır.",
        "☁️ Yumşaq hava var, planlarını rahat qura bilərsən."
    ],
    "cool": [
        "🧥 Hava sərin ola bilər. Nazik gödəkcə faydalı olar.",
        "🌬 Sərinlik hiss edilə bilər, xüsusən səhər və axşam saatlarında.",
        "☕ Bu havada isti içki əla gedər."
    ],
    "cold": [
        "❄️ Hava soyuqdur. Qalın geyinmək məsləhətdir.",
        "🧣 Soyuq hava var. Özünü isti saxla.",
        "🔥 Külək də varsa, hiss edilən temperatur daha aşağı ola bilər."
    ]
}

# Place ID xəritəsi – callback_data limit problemi yaşamamaq üçün
PLACE_MAP: Dict[str, Dict[str, str]] = {}
REGION_ID_TO_NAME: Dict[str, str] = {}

# Geocoding cache
GEO_CACHE: Dict[str, Tuple[float, float, str]] = {}

# =========================
# HELPERS
# =========================
def norm(s: str) -> str:
    return " ".join((s or "").strip().split())

def escape_md(text: str) -> str:
    # MarkdownV2 istifadə etmirik, klassik Markdown var.
    # Ona görə sadə qaytarırıq.
    return str(text)

def build_ids() -> None:
    region_no = 1
    place_no = 1
    for region_name, data in REGIONS.items():
        region_id = f"r{region_no}"
        REGION_ID_TO_NAME[region_id] = region_name
        region_no += 1

        all_places = data["Şəhər"] + data["Qəsəbələr"] + data["Kəndlər"]
        for place in all_places:
            place_id = f"p{place_no}"
            PLACE_MAP[place_id] = {
                "region": region_name,
                "place": place,
            }
            place_no += 1

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

def weather_tip(temp: float) -> str:
    if temp >= 35:
        pool = WEATHER_QUOTES["hot"]
    elif temp >= 25:
        pool = WEATHER_QUOTES["warm"]
    elif temp >= 15:
        pool = WEATHER_QUOTES["mild"]
    elif temp >= 5:
        pool = WEATHER_QUOTES["cool"]
    else:
        pool = WEATHER_QUOTES["cold"]
    return random.choice(pool)

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
    """, (chat_id, username or "", first_name or "", last_name or ""))
    conn.commit()
    conn.close()

def set_last_place(chat_id: int, place_id: str) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET last_place_id=?, updated_at=CURRENT_TIMESTAMP
        WHERE chat_id=?
    """, (place_id, chat_id))
    conn.commit()
    conn.close()

def set_favorite_place(chat_id: int, place_id: str, daily_enabled: int = 1) -> None:
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

def enable_daily(chat_id: int) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET daily_enabled=1, updated_at=CURRENT_TIMESTAMP
        WHERE chat_id=?
    """, (chat_id,))
    conn.commit()
    conn.close()

def get_user(chat_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row

def get_subscribers() -> List[sqlite3.Row]:
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

def load_geocache(cache_key: str) -> Optional[Tuple[float, float, str]]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT lat, lon, resolved_name FROM geocache WHERE cache_key=?", (cache_key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return (float(row["lat"]), float(row["lon"]), str(row["resolved_name"] or ""))

# =========================
# HTTP / WEATHER
# =========================
def _http_get_json(url: str, params: dict, timeout: int = 20):
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()

async def safe_get_json(url: str, params: dict, timeout: int = 20):
    import asyncio
    return await asyncio.to_thread(_http_get_json, url, params, timeout)

async def geocode_place(place: str, region: Optional[str] = None) -> Optional[Tuple[float, float, str]]:
    place = norm(place)
    region = norm(region or "")
    cache_key = f"{place}|{region}"

    if cache_key in GEO_CACHE:
        return GEO_CACHE[cache_key]

    db_cached = load_geocache(cache_key)
    if db_cached:
        GEO_CACHE[cache_key] = db_cached
        return db_cached

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

    geocode_url = "http://api.openweathermap.org/geo/1.0/direct"

    for q in queries:
        try:
            data = await safe_get_json(
                geocode_url,
                {
                    "q": q,
                    "limit": 5,
                    "appid": OWM_API_KEY
                }
            )

            if not isinstance(data, list) or not data:
                continue

            best = None
            wanted_region = region.lower().replace(" rayonu", "").replace(" şəhəri", "").strip()

            for item in data:
                name = norm(item.get("name", ""))
                state = norm(item.get("state", ""))
                country = norm(item.get("country", ""))

                score = 0
                if country.upper() == "AZ":
                    score += 5
                if place.lower() == name.lower():
                    score += 6
                elif place.lower() in name.lower():
                    score += 4

                state_low = state.lower()
                if wanted_region and wanted_region in state_low:
                    score += 5

                if "nakhchivan" in state_low or "naxçıvan" in state_low:
                    score += 4

                lat = item.get("lat")
                lon = item.get("lon")
                if lat is None or lon is None:
                    continue

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
            logger.warning("Geocoding error for %s / %s: %s", place, region, e)

    return None

async def fetch_current_weather(lat: float, lon: float) -> Optional[dict]:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        data = await safe_get_json(
            url,
            {
                "lat": lat,
                "lon": lon,
                "appid": OWM_API_KEY,
                "units": "metric",
                "lang": "az"
            }
        )
        return data
    except Exception as e:
        logger.warning("Current weather fetch error (%s, %s): %s", lat, lon, e)
        return None

async def fetch_forecast(lat: float, lon: float) -> Optional[dict]:
    # 5 günlük / 3 saatlıq forecast endpoint
    try:
        url = "https://api.openweathermap.org/data/2.5/forecast"
        data = await safe_get_json(
            url,
            {
                "lat": lat,
                "lon": lon,
                "appid": OWM_API_KEY,
                "units": "metric",
                "lang": "az"
            }
        )
        return data
    except Exception as e:
        logger.warning("Forecast fetch error (%s, %s): %s", lat, lon, e)
        return None

def summarize_forecast(forecast_data: Optional[dict]) -> str:
    if not forecast_data or "list" not in forecast_data:
        return "📌 Yaxın saatlar üçün əlavə proqnoz hazırda göstərilə bilmədi."

    items = forecast_data.get("list", [])[:3]
    if not items:
        return "📌 Yaxın saatlar üçün əlavə proqnoz tapılmadı."

    parts = []
    for item in items:
        dt_txt = str(item.get("dt_txt", ""))[11:16]
        main = item.get("main", {}) or {}
        weather = (item.get("weather", [{}]) or [{}])[0]
        temp = main.get("temp", "?")
        desc = str(weather.get("description", ""))
        icon = pick_icon(desc)
        parts.append(f"• {dt_txt} — {icon} {temp}°C, {desc}")

    return "🕒 Yaxın saatlar:\n" + "\n".join(parts)

def build_weather_message(place: str, region: str, weather_data: dict, forecast_data: Optional[dict]) -> str:
    main = weather_data.get("main", {}) or {}
    wind = weather_data.get("wind", {}) or {}
    clouds = weather_data.get("clouds", {}) or {}
    sys_data = weather_data.get("sys", {}) or {}
    weather_item = (weather_data.get("weather", [{}]) or [{}])[0]

    temp = main.get("temp")
    feels = main.get("feels_like")
    temp_min = main.get("temp_min")
    temp_max = main.get("temp_max")
    humidity = main.get("humidity")
    pressure = main.get("pressure")
    wind_speed = wind.get("speed")
    cloudiness = clouds.get("all")
    desc = str(weather_item.get("description", "Məlumat yoxdur")).lower()
    icon = pick_icon(desc)

    tip_text = weather_tip(float(temp)) if temp is not None else "Günün xoş keçsin."

    sunrise = sys_data.get("sunrise")
    sunset = sys_data.get("sunset")

    # sadə görünüş – sunrise/sunset varsa UTC timestamp gəlir, çevirmədən çox qarışdırmıram
    extra = []
    if temp_min is not None and temp_max is not None:
        extra.append(f"📉 Min/Max: {temp_min}°C / {temp_max}°C")
    if cloudiness is not None:
        extra.append(f"☁️ Buludluluq: {cloudiness}%")
    if pressure is not None:
        extra.append(f"🧭 Təzyiq: {pressure} hPa")

    forecast_text = summarize_forecast(forecast_data)

    msg = (
        f"📍 *{escape_md(place)}*\n"
        f"🗺 Rayon/Bölgə: *{escape_md(region)}*\n\n"
        f"{icon} *Cari hava:* {escape_md(desc)}\n"
        f"🌡 *Temperatur:* {temp}°C\n"
        f"🤗 *Hiss edilən:* {feels}°C\n"
        f"💧 *Rütubət:* {humidity}%\n"
        f"🌬 *Külək:* {wind_speed} m/s\n"
    )

    if extra:
        msg += "\n" + "\n".join(extra)

    msg += (
        f"\n\n{forecast_text}\n\n"
        f"✨ *Qısa tövsiyə:* {escape_md(tip_text)}"
    )

    return msg

# =========================
# KEYBOARDS
# =========================
def build_regions_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for region_name, data in REGIONS.items():
        for city in data["Şəhər"]:
            pid = get_place_id(region_name, city)
            if pid:
                keyboard.append([
                    InlineKeyboardButton(f"🏙 {city}", callback_data=f"weather|{pid}")
                ])
    keyboard.append([InlineKeyboardButton("⭐ Sevimli məkanım", callback_data="myfav")])
    return InlineKeyboardMarkup(keyboard)

def build_places_keyboard(region: str) -> InlineKeyboardMarkup:
    places = REGIONS[region]["Qəsəbələr"] + REGIONS[region]["Kəndlər"]
    keyboard = []
    row = []
    for idx, place in enumerate(places, start=1):
        pid = get_place_id(region, place)
        if not pid:
            continue
        row.append(InlineKeyboardButton(place, callback_data=f"weather|{pid}"))
        if idx % 2 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("⬅️ Şəhərlər", callback_data="regions")])
    keyboard.append([InlineKeyboardButton("⭐ Sevimli məkanım", callback_data="myfav")])
    return InlineKeyboardMarkup(keyboard)

def build_after_weather_keyboard(place_id: str) -> InlineKeyboardMarkup:
    item = PLACE_MAP.get(place_id, {})
    region = item.get("region", "")
    keyboard = [
        [
            InlineKeyboardButton("⭐ Sevimli et", callback_data=f"fav|{place_id}"),
            InlineKeyboardButton("🔔 09:00 aktiv et", callback_data=f"dailyon|{place_id}")
        ],
        [
            InlineKeyboardButton("🔕 09:00 söndür", callback_data="dailyoff"),
        ]
    ]

    if region:
        keyboard.append([InlineKeyboardButton("📍 Bu rayonun kəndləri", callback_data=f"regionplaces|{region}")])

    keyboard.append([InlineKeyboardButton("🏙 Şəhərlər", callback_data="regions")])
    return InlineKeyboardMarkup(keyboard)

# =========================
# SCHEDULE / DAILY
# =========================
async def daily_weather_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data or {}
    chat_id = int(job_data.get("chat_id"))
    place_id = str(job_data.get("place_id"))

    item = PLACE_MAP.get(place_id)
    if not item:
        logger.warning("Job skipped, unknown place_id=%s", place_id)
        return

    place = item["place"]
    region = item["region"]

    geo = await geocode_place(place, region)
    if not geo:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ {place} üçün bu gün koordinat tapılmadı. Sonra yenidən yoxlanacaq."
        )
        return

    lat, lon, _ = geo
    current = await fetch_current_weather(lat, lon)
    forecast = await fetch_forecast(lat, lon)

    if not current:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ {place} üçün hava məlumatı hazırda gətirilə bilmədi."
        )
        return

    message = (
        f"⏰ *Saat 09:00 gündəlik hava məlumatı*\n\n"
        + build_weather_message(place, region, current, forecast)
    )

    await context.bot.send_message(
        chat_id=chat_id,
        text=message,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_after_weather_keyboard(place_id)
    )

def remove_existing_daily_jobs(application: Application, chat_id: int) -> None:
    name = f"daily_{chat_id}"
    current_jobs = application.job_queue.get_jobs_by_name(name)
    for job in current_jobs:
        job.schedule_removal()

def schedule_user_daily_job(application: Application, chat_id: int, place_id: str) -> None:
    remove_existing_daily_jobs(application, chat_id)
    application.job_queue.run_daily(
        callback=daily_weather_job,
        time=dtime(hour=9, minute=0, tzinfo=ZoneInfo(TIMEZONE)),
        data={"chat_id": chat_id, "place_id": place_id},
        name=f"daily_{chat_id}",
        chat_id=chat_id
    )

def restore_all_daily_jobs(application: Application) -> None:
    rows = get_subscribers()
    for row in rows:
        try:
            schedule_user_daily_job(application, int(row["chat_id"]), str(row["favorite_place_id"]))
            logger.info("Daily job restored for chat_id=%s", row["chat_id"])
        except Exception as e:
            logger.exception("Failed to restore job for chat_id=%s: %s", row["chat_id"], e)

# =========================
# HEALTH SERVER
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(HEALTH_TEXT.encode("utf-8"))

    def log_message(self, format, *args):
        return

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        logger.info("Health server started on port %s", PORT)
        server.serve_forever()
    except Exception as e:
        logger.exception("Health server error: %s", e)

# =========================
# CORE SEND WEATHER
# =========================
async def send_weather_for_place(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    place_id: str,
    prefix: str = ""
) -> None:
    item = PLACE_MAP.get(place_id)
    if not item:
        target = update.effective_message or update.callback_query.message
        await target.reply_text("❌ Məkan tapılmadı.")
        return

    place = item["place"]
    region = item["region"]

    chat = update.effective_chat
    user = update.effective_user
    if chat and user:
        upsert_user(
            chat_id=chat.id,
            username=user.username or "",
            first_name=user.first_name or "",
            last_name=user.last_name or ""
        )
        set_last_place(chat.id, place_id)

    geo = await geocode_place(place, region)
    if not geo:
        text = (
            f"❌ *{place}* üçün koordinat tapılmadı.\n\n"
            f"Rayon: *{region}*\n"
            f"Bu məkan üçün OpenWeather geocoding nəticə vermədi. İstəsən sonra bu məkan üçün manual koordinat da əlavə edə bilərik."
        )
        target = update.effective_message or update.callback_query.message
        await target.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        return

    lat, lon, _ = geo
    current = await fetch_current_weather(lat, lon)
    forecast = await fetch_forecast(lat, lon)

    if not current:
        target = update.effective_message or update.callback_query.message
        await target.reply_text("⚠️ Hava məlumatı hazırda gətirilə bilmədi. Bir az sonra yenidən yoxla.")
        return

    text = prefix + build_weather_message(place, region, current, forecast)
    target = update.effective_message or update.callback_query.message
    await target.reply_text(
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_after_weather_keyboard(place_id)
    )

# =========================
# COMMANDS
# =========================
async def post_init(app: Application) -> None:
    commands = [
        BotCommand("start", "Botu başlat"),
        BotCommand("menu", "Əsas menyu"),
        BotCommand("help", "Kömək"),
        BotCommand("fav", "Sevimli məkanımı göstər"),
        BotCommand("dailyon", "09:00 hava bildirişini aktiv et"),
        BotCommand("dailyoff", "09:00 hava bildirişini söndür"),
    ]
    await app.bot.set_my_commands(commands)
    restore_all_daily_jobs(app)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if user and chat:
        upsert_user(
            chat_id=chat.id,
            username=user.username or "",
            first_name=user.first_name or "",
            last_name=user.last_name or ""
        )

    text = (
        "✨ *Salam! Naxçıvan Hava Botuna xoş gəlmisən.*\n\n"
        "Bu bot ilə:\n"
        "• şəhər, kənd və qəsəbə üzrə hava məlumatı ala bilərsən\n"
        "• seçdiyin məkanı sevimli kimi yadda saxlaya bilərsən\n"
        "• hər gün *saat 09:00*-da avtomatik hava məlumatı ala bilərsən\n\n"
        "Aşağıdan şəhər seç."
    )
    await update.message.reply_text(
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_regions_keyboard()
    )

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🏙 Şəhəri seç:",
        reply_markup=build_regions_keyboard()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "*İstifadə qaydası*\n\n"
        "1. Şəhər seç\n"
        "2. İstəsən həmin rayonun kənd/qəsəbələrinə keç\n"
        "3. Hava məlumatından sonra `⭐ Sevimli et` seç\n"
        "4. `🔔 09:00 aktiv et` ilə hər gün avtomatik məlumat al\n\n"
        "*Komandalar*\n"
        "/start – başlat\n"
        "/menu – əsas seçim menyusu\n"
        "/fav – sevimli məkan hava məlumatı\n"
        "/dailyon – son və ya sevimli məkan üçün 09:00 aktiv et\n"
        "/dailyoff – 09:00 bildirişini söndür"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def fav_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    row = get_user(chat_id)
    if not row or not row["favorite_place_id"]:
        await update.message.reply_text("⭐ Hələ sevimli məkan seçilməyib.")
        return

    await send_weather_for_place(
        update=update,
        context=context,
        place_id=str(row["favorite_place_id"]),
        prefix="⭐ *Sevimli məkanın üçün hava məlumatı*\n\n"
    )

async def dailyon_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    row = get_user(chat_id)

    if not row:
        await update.message.reply_text("⚠️ Əvvəl /start yaz və bir məkan seç.")
        return

    place_id = None
    if row["favorite_place_id"]:
        place_id = str(row["favorite_place_id"])
    elif row["last_place_id"]:
        place_id = str(row["last_place_id"])

    if not place_id:
        await update.message.reply_text("⚠️ Əvvəlcə bir şəhər/kənd/qəsəbə seç.")
        return

    set_favorite_place(chat_id, place_id, daily_enabled=1)
    schedule_user_daily_job(context.application, chat_id, place_id)

    item = PLACE_MAP.get(place_id, {})
    await update.message.reply_text(
        f"🔔 Gündəlik hava bildirişi aktiv edildi.\nSaat 09:00-da *{item.get('place', 'məkan')}* üçün məlumat göndəriləcək.",
        parse_mode=ParseMode.MARKDOWN
    )

async def dailyoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    disable_daily(chat_id)
    remove_existing_daily_jobs(context.application, chat_id)
    await update.message.reply_text("🔕 Saat 09:00 gündəlik bildiriş söndürüldü.")

# =========================
# TEXT SEARCH
# =========================
def find_place_by_text(text: str) -> Optional[str]:
    q = norm(text).lower()
    if not q:
        return None

    # tam uyğunluq
    for pid, item in PLACE_MAP.items():
        if norm(item["place"]).lower() == q:
            return pid

    # qismən uyğunluq
    for pid, item in PLACE_MAP.items():
        if q in norm(item["place"]).lower():
            return pid

    # region adı ilə şəhəri qaytar
    for region_name, data in REGIONS.items():
        if q == norm(region_name).lower():
            if data["Şəhər"]:
                return get_place_id(region_name, data["Şəhər"][0])

    return None

async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    pid = find_place_by_text(text)
    if pid:
        await send_weather_for_place(update, context, pid)
        return

    await update.message.reply_text(
        "❌ Məkanı tanımadım.\nAşağıdan siyahıdan seç:",
        reply_markup=build_regions_keyboard()
    )

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
        await send_weather_for_place(
            update=update,
            context=context,
            place_id=str(row["favorite_place_id"]),
            prefix="⭐ *Sevimli məkanın üçün hava məlumatı*\n\n"
        )
        return

    if data.startswith("weather|"):
        place_id = data.split("|", 1)[1]
        await send_weather_for_place(update, context, place_id)

        item = PLACE_MAP.get(place_id)
        if item:
            region = item["region"]
            if item["place"] in REGIONS[region]["Şəhər"]:
                await query.message.reply_text(
                    f"📌 *{item['place']}* üçün aid kənd və qəsəbələr:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=build_places_keyboard(region)
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
        set_favorite_place(update.effective_chat.id, place_id, daily_enabled=1)
        item = PLACE_MAP.get(place_id, {})
        await query.message.reply_text(
            f"⭐ *{item.get('place', 'Məkan')}* sevimli kimi yadda saxlanıldı.\n"
            f"🔔 Gündəlik 09:00 bildirişi də aktiv edildi.",
            parse_mode=ParseMode.MARKDOWN
        )
        schedule_user_daily_job(context.application, update.effective_chat.id, place_id)
        return

    if data.startswith("dailyon|"):
        place_id = data.split("|", 1)[1]
        set_favorite_place(update.effective_chat.id, place_id, daily_enabled=1)
        schedule_user_daily_job(context.application, update.effective_chat.id, place_id)
        item = PLACE_MAP.get(place_id, {})
        await query.message.reply_text(
            f"🔔 *{item.get('place', 'Məkan')}* üçün hər gün saat 09:00 bildirişi aktiv edildi.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if data == "dailyoff":
        disable_daily(update.effective_chat.id)
        remove_existing_daily_jobs(context.application, update.effective_chat.id)
        await query.message.reply_text("🔕 Saat 09:00 gündəlik bildiriş söndürüldü.")
        return

    await query.message.reply_text("⚠️ Naməlum əməliyyat.")

# =========================
# ERROR HANDLER
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception: %s", context.error)

    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Gözlənilməz xəta baş verdi. Zəhmət olmasa yenidən yoxla."
            )
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

    build_ids()
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
