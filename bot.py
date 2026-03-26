import os
import json
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWM_API_KEY = os.getenv("OWM_API_KEY", "").strip()

# Render / deployment config
USE_WEBHOOK = os.getenv("USE_WEBHOOK", "0").strip() == "1"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()      # məsələn: https://your-app.onrender.com
PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram").strip()

# Logging
logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# İstifadəçilərin xoş gəldin mesaj vaxtı
welcome_last_shown: Dict[int, datetime] = {}

# Rayon və şəhərlər + kənd/qəsəbələr
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

# İstəsən əlavə əl koordinatları da saxlaya bilərsən.
# Geocoding tapa bilməyəndə fallback kimi işləyəcək.
MANUAL_COORDS: Dict[str, Tuple[float, float]] = {
    "Naxçıvan (şəhər)": (39.2090, 45.4120),
    "Əliabad (qəsəbə)": (39.2167, 45.3833),
    "Babək (şəhər)": (39.1500, 45.4500),
    "Cəhri": (39.1167, 45.4833),
    "Nehrəm": (39.0833, 45.5000),
}

WEATHER_ICONS = {
    "clear": "☀️",
    "clouds": "☁️",
    "few clouds": "🌤",
    "scattered clouds": "🌤",
    "broken clouds": "☁️",
    "rain": "🌧",
    "shower rain": "🌧",
    "thunderstorm": "⛈",
    "snow": "❄️",
    "mist": "🌫",
    "fog": "🌫",
    "drizzle": "🌦",
}

WEATHER_QUOTES = {
    "hot": [
        "🔥 İsti gündür, su içməyi unutma!",
        "☀️ Günəş güclüdür, kölgə və su ən yaxşı dostundur.",
        "🥵 Bu havada yüngül geyim daha rahat olar.",
    ],
    "warm": [
        "🌞 Hava xoşdur, gəzinti üçün əla vaxtdır.",
        "🌼 Açıq hava planı üçün uyğun gündür.",
        "🍀 Bu hava adama enerji verir.",
    ],
    "mild": [
        "🌤 Rahat havadır, günün xoş keçsin.",
        "😊 Nə isti, nə soyuq — ideal hava.",
        "☁️ Sakit və rahat bir gündür.",
    ],
    "cool": [
        "🧥 Hava sərin ola bilər, nazik üst geyim yaxşı olar.",
        "🌬 Küləkli/sərin hiss edilə bilər.",
        "☕ Bu havada isti çay əla gedər.",
    ],
    "cold": [
        "❄️ Hava soyuqdur, qalın geyinmək yaxşı olar.",
        "🧣 Soyuq havadır, özünü qoru.",
        "🔥 İsti qalmaq üçün uyğun geyin.",
    ],
}

# Rayon ID xəritəsi (callback_data qısa olsun deyə)
REGION_IDS = {str(i): region for i, region in enumerate(REGIONS.keys(), start=1)}
REGION_IDS_REVERSE = {v: k for k, v in REGION_IDS.items()}

# Sadə cache — hər dəfə eyni geocoding sorğusu getməsin
GEO_CACHE: Dict[str, Tuple[float, float, str]] = {}


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().split())


def weather_alert(temp: float) -> str:
    if temp >= 35:
        quotes = WEATHER_QUOTES["hot"]
    elif temp >= 25:
        quotes = WEATHER_QUOTES["warm"]
    elif temp >= 15:
        quotes = WEATHER_QUOTES["mild"]
    elif temp >= 5:
        quotes = WEATHER_QUOTES["cool"]
    else:
        quotes = WEATHER_QUOTES["cold"]
    return random.choice(quotes)


def pick_weather_icon(desc: str) -> str:
    desc = (desc or "").lower()
    for key, emoji in WEATHER_ICONS.items():
        if key in desc:
            return emoji
    return "🌡"


def build_regions_keyboard() -> InlineKeyboardMarkup:
    keyboard = []
    for region, places in REGIONS.items():
        for city in places["Şəhər"]:
            region_id = REGION_IDS_REVERSE[region]
            keyboard.append([
                InlineKeyboardButton(city, callback_data=f"city|{region_id}|{city}")
            ])
    return InlineKeyboardMarkup(keyboard)


def build_places_keyboard(region: str) -> InlineKeyboardMarkup:
    places = REGIONS[region]
    all_places = places["Qəsəbələr"] + places["Kəndlər"]

    keyboard = []
    row = []
    region_id = REGION_IDS_REVERSE[region]

    for i, place in enumerate(all_places, start=1):
        row.append(
            InlineKeyboardButton(
                place,
                callback_data=f"place|{region_id}|{place}"
            )
        )
        if i % 2 == 0:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("⬅️ Rayon/şəhər seçimi", callback_data="back|regions")])
    return InlineKeyboardMarkup(keyboard)


def get_region_search_variants(region: str) -> List[str]:
    return [
        region,
        region.replace("rayonu", "").strip(),
        region.replace("şəhəri", "").strip(),
        "Naxçıvan Muxtar Respublikası",
        "Nakhchivan Autonomous Republic",
        "Nakhchivan",
        "Azerbaijan",
    ]


def geocode_place(place: str, region: Optional[str] = None) -> Optional[Tuple[float, float, str]]:
    place = normalize_text(place)
    region = normalize_text(region or "")

    cache_key = f"{place}|{region}"
    if cache_key in GEO_CACHE:
        return GEO_CACHE[cache_key]

    # 1) Əl koordinatı varsa əvvəl onu qaytar
    if place in MANUAL_COORDS:
        lat, lon = MANUAL_COORDS[place]
        result = (lat, lon, place)
        GEO_CACHE[cache_key] = result
        return result

    # 2) Geocoding query variantları
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
            response = requests.get(
                geocode_url,
                params={
                    "q": q,
                    "limit": 5,
                    "appid": OWM_API_KEY,
                },
                timeout=20,
            )
            response.raise_for_status()
            results = response.json()

            if not isinstance(results, list) or not results:
                continue

            # Ən uyğun nəticəni seç
            best = None
            for item in results:
                name = normalize_text(item.get("name", ""))
                state = normalize_text(item.get("state", ""))
                country = normalize_text(item.get("country", ""))

                score = 0
                if country.upper() == "AZ":
                    score += 5
                if place.lower() in name.lower():
                    score += 5
                if region and region.lower().replace(" rayonu", "") in state.lower():
                    score += 3
                if "nakhchivan" in state.lower() or "naxçıvan" in state.lower():
                    score += 3

                candidate = {
                    "score": score,
                    "lat": item.get("lat"),
                    "lon": item.get("lon"),
                    "label": name or place,
                }

                if best is None or candidate["score"] > best["score"]:
                    best = candidate

            if best and best["lat"] is not None and best["lon"] is not None:
                result = (float(best["lat"]), float(best["lon"]), best["label"])
                GEO_CACHE[cache_key] = result
                return result

        except Exception as e:
            logger.warning("Geocoding error for %s | %s: %s", place, region, e)

    return None


def fetch_weather_by_coords(lat: float, lon: float) -> Optional[dict]:
    weather_url = "https://api.openweathermap.org/data/2.5/weather"
    try:
        response = requests.get(
            weather_url,
            params={
                "lat": lat,
                "lon": lon,
                "appid": OWM_API_KEY,
                "units": "metric",
                "lang": "az",
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning("Weather fetch error (%s, %s): %s", lat, lon, e)
        return None


def format_weather_text(place: str, data: dict) -> str:
    main = data.get("main", {}) or {}
    weather_list = data.get("weather", []) or [{}]
    wind = data.get("wind", {}) or {}

    temp = main.get("temp")
    feels_like = main.get("feels_like")
    humidity = main.get("humidity")
    pressure = main.get("pressure")
    desc = str(weather_list[0].get("description", "Məlumat yoxdur")).lower()
    wind_speed = wind.get("speed", 0)

    icon = pick_weather_icon(desc)
    alert_text = weather_alert(float(temp)) if temp is not None else "Günün xoş keçsin."

    return (
        f"📍 *{place}*\n\n"
        f"{icon} *Hava vəziyyəti:* {desc}\n"
        f"🌡 *Temperatur:* {temp}°C\n"
        f"🤗 *Hiss edilən:* {feels_like}°C\n"
        f"💧 *Rütubət:* {humidity}%\n"
        f"🌬 *Külək:* {wind_speed} m/s\n"
        f"🧭 *Təzyiq:* {pressure} hPa\n\n"
        f"✨ {alert_text}"
    )


async def send_weather(update: Update, place: str, region: Optional[str] = None) -> None:
    place = normalize_text(place)
    region = normalize_text(region or "")

    geo = geocode_place(place, region)
    if not geo:
        text = (
            f"❌ *{place}* üçün koordinat tapılmadı.\n\n"
            f"Yoxla:\n"
            f"- adı fərqli yazılıbsa başqa variantla yaz\n"
            f"- /start vurub siyahıdan yenidən seç\n"
            f"- istəsən bu məkan üçün əl koordinatı da əlavə edə bilərik"
        )
    else:
        lat, lon, resolved_name = geo
        weather_data = fetch_weather_by_coords(lat, lon)

        if not weather_data:
            text = "⚠️ Hava məlumatını gətirmək mümkün olmadı. Bir az sonra yenidən yoxla."
        else:
            shown_name = place if place else resolved_name
            text = format_weather_text(shown_name, weather_data)

    if update.callback_query:
        await update.callback_query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    elif update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    now = datetime.now()

    if (
        user_id not in welcome_last_shown
        or now - welcome_last_shown[user_id] >= timedelta(hours=1)
    ):
        welcome_text = (
            "✨ *Salam! Naxçıvan Hava Botuna xoş gəlmisiniz!*\n\n"
            "📍 Əvvəlcə şəhəri seçin.\n"
            "🏘 Sonra həmin şəhər/rayona aid kənd və qəsəbələr görünəcək.\n"
            "🌦 Seçim etdikdən sonra hava məlumatı göstəriləcək."
        )
        await update.message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
        welcome_last_shown[user_id] = now

    await update.message.reply_text(
        "🏙 *Şəhəri seçin:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_regions_keyboard()
    )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""

    if data == "back|regions":
        await query.message.reply_text("🏙 Şəhəri seçin:", reply_markup=build_regions_keyboard())
        return

    parts = data.split("|")
    action = parts[0] if len(parts) > 0 else ""

    try:
        if action == "city" and len(parts) >= 3:
            region_id = parts[1]
            city = parts[2]
            region = REGION_IDS.get(region_id)

            await send_weather(update, city, region)

            if region:
                await query.message.reply_text(
                    f"📌 *{city}* üçün aid kənd və qəsəbələr:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=build_places_keyboard(region)
                )

        elif action == "place" and len(parts) >= 3:
            region_id = parts[1]
            place = parts[2]
            region = REGION_IDS.get(region_id)
            await send_weather(update, place, region)

        else:
            await query.message.reply_text("⚠️ Naməlum seçim.")
    except Exception as e:
        logger.exception("Callback error: %s", e)
        await query.message.reply_text("⚠️ Sorğu işlənərkən xəta baş verdi.")


def find_place_region_by_text(user_text: str) -> Optional[Tuple[str, Optional[str]]]:
    text = normalize_text(user_text).lower()

    # Əvvəl REGIONS içində tam uyğunluq axtar
    for region, data in REGIONS.items():
        all_places = data["Şəhər"] + data["Qəsəbələr"] + data["Kəndlər"]
        for place in all_places:
            if normalize_text(place).lower() == text:
                return place, region

    # Sonra qismən uyğunluq
    for region, data in REGIONS.items():
        all_places = data["Şəhər"] + data["Qəsəbələr"] + data["Kəndlər"]
        for place in all_places:
            if text in normalize_text(place).lower():
                return place, region

    # Heç nə tapılmadısa, sərbəst mətn kimi qaytar
    if text:
        return user_text.strip(), None

    return None


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_text = (update.message.text or "").strip()
    if not user_text:
        await update.message.reply_text("⚠️ Mətn boşdur.")
        return

    match = find_place_region_by_text(user_text)
    if not match:
        await update.message.reply_text(
            "❌ Məkanı tanımadım.\n\n🏙 Yenidən siyahıdan seç:",
            reply_markup=build_regions_keyboard()
        )
        return

    place, region = match
    await send_weather(update, place, region)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception: %s", context.error)


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN tapılmadı.")
    if not OWM_API_KEY:
        raise RuntimeError("OWM_API_KEY tapılmadı.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    app.add_error_handler(error_handler)

    return app


def main() -> None:
    app = build_app()

    logger.info("Bot starts. USE_WEBHOOK=%s", USE_WEBHOOK)

    # Render üçün ən rahat variant çox vaxt polling-dir
    if not USE_WEBHOOK:
        logger.info("Running with polling...")
        app.run_polling(drop_pending_updates=True)
        return

    # Webhook rejimi
    if not WEBHOOK_URL:
        raise RuntimeError("USE_WEBHOOK=1 olduqda WEBHOOK_URL mütləq verilməlidir.")

    webhook_url = f"{WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}"
    logger.info("Running with webhook: %s", webhook_url)

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=WEBHOOK_PATH.lstrip("/"),
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
