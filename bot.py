"""
Brawl Stars Club Statistics Tracker Bot
Automatically tracks 3 clubs and posts daily analytics
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import pytz

import aiohttp
from telegram import Bot, Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import TelegramError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Configuration
TELEGRAM_BOT_TOKEN = "8305841062:AAHTtEmmXR_YOOOH2J7JdVqb7QD47zefgoQ"
BRAWLSTARS_API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiIsImtpZCI6IjI4YTMxOGY3LTAwMDAtYTFlYi03ZmExLTJjNzQzM2M2Y2NhNSJ9.eyJpc3MiOiJzdXBlcmNlbGwiLCJhdWQiOiJzdXBlcmNlbGw6Z2FtZWFwaSIsImp0aSI6IjcyZThkZmNkLTU5ZWUtNDA3NS1hNzMyLWZlZGVkMjliMTRmMCIsImlhdCI6MTc3Mjk0MjMyMCwic3ViIjoiZGV2ZWxvcGVyLzVjM2Q1YmU4LWNlYjYtYWUzMi1jZjkwLTA1ZjFhZjU5MzNhNSIsInNjb3BlcyI6WyJicmF3bHN0YXJzIl0sImxpbWl0cyI6W3sidGllciI6ImRldmVsb3Blci9zaWx2ZXIiLCJ0eXBlIjoidGhyb3R0bGluZyJ9LHsiY2lkcnMiOlsiMTYyLjEyMC4xODguMjU1Il0sInR5cGUiOiJjbGllbnQifV19.hP2_kEkIi-HA4ZShdjISztzJf1bZGbX_iX6E_gyQE-bhgTdrs_cPtqc-nH8AaCUkIusWLYNO3aioKm7Jnqg95w"

# Чаты для автоматических отчетов (опционально)
CHAT_IDS = ["-1002863195632", "-1002655366005"]

# Разрешить боту работать в любых чатах
ALLOW_ANY_CHAT = True

# Club configuration
CLUBS = {
    "Ignis Noctis": "JQRUUO2C",
    "Glacial Noctis": "JGYCOJLV",
    "Abyssys Noctis": "JRPPU999",
    "Silva Noctis": "2Q8C0G0V0"
}

# API endpoints
BS_API_BASE = "https://api.brawlstars.com/v1"

# Timezone
MSK = pytz.timezone('Europe/Moscow')

# Data storage files
DAILY_DATA_FILE = "daily_stats.json"
WEEKLY_DATA_FILE = "weekly_stats.json"
MONTHLY_DATA_FILE = "monthly_stats.json"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BrawlStarsAPI:
    """Handles all Brawl Stars API interactions with retry logic"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json"
        }
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _make_request(self, endpoint: str, retries: int = 3) -> dict:
        """Выполнить API запрос с логикой повторных попыток"""
        for attempt in range(retries):
            try:
                async with self.session.get(f"{BS_API_BASE}{endpoint}") as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Ограничение частоты запросов
                        wait_time = 2 ** attempt
                        logger.warning(f"Ограничение частоты запросов, ожидание {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Ошибка API: {response.status}")
                        await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Запрос не удался (попытка {attempt + 1}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None
    
    async def get_club_info(self, club_tag: str) -> dict:
        """Получить информацию о клубе"""
        # Удалить # если присутствует и закодировать
        tag = club_tag.replace('#', '')
        encoded_tag = f"%23{tag}"
        return await self._make_request(f"/clubs/{encoded_tag}")
    
    async def get_club_members(self, club_tag: str) -> List[dict]:
        """Получить членов клуба"""
        club_data = await self.get_club_info(club_tag)
        if club_data and 'members' in club_data:
            return club_data['members']
        return []


class DataStorage:
    """Управление сохранением данных"""
    
    @staticmethod
    def load_data(filename: str) -> dict:
        """Загрузить данные из JSON файла"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content:
                        return json.loads(content)
                    else:
                        logger.debug(f"{filename} пуст, возвращаю пустой словарь")
                        return {}
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования {filename}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Ошибка загрузки {filename}: {e}")
        return {}
    
    @staticmethod
    def save_data(filename: str, data: dict):
        """Сохранить данные в JSON файл"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Данные сохранены в {filename}")
        except Exception as e:
            logger.error(f"Ошибка сохранения {filename}: {e}")


class StatsCalculator:
    """Calculate statistics and changes"""
    
    @staticmethod
    def calculate_trophy_change(current_trophies: int, previous_trophies: int) -> int:
        """Calculate trophy difference"""
        return current_trophies - previous_trophies
    
    @staticmethod
    def get_top_players(players_data: List[Tuple[str, str, int]], count: int = 3) -> List[Tuple[str, str, int]]:
        """Get top N players by trophy change"""
        sorted_players = sorted(players_data, key=lambda x: x[2], reverse=True)
        return sorted_players[:count]
    
    @staticmethod
    def format_trophy_change(change: int) -> str:
        """Format trophy change with + or -"""
        if change > 0:
            return f"+{change}"
        return str(change)
    
    @staticmethod
    def get_top_players_by_trophies(players_data: List[Tuple[str, str, int]], count: int = 10) -> List[Tuple[str, str, int]]:
        """Get top N players by total trophies"""
        sorted_players = sorted(players_data, key=lambda x: x[2], reverse=True)
        return sorted_players[:count]
    
    @staticmethod
    def get_trend_emoji(change: int) -> str:
        """Get emoji based on trophy change trend"""
        if change > 100:
            return "📈📈📈"
        elif change > 50:
            return "📈📈"
        elif change > 0:
            return "📈"
        elif change < -100:
            return "📉📉📉"
        elif change < -50:
            return "📉📉"
        elif change < 0:
            return "📉"
        else:
            return "➡️"


class BrawlStarsBot:
    """Main bot class"""
    
    def __init__(self):
        self.application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        self.bot = self.application.bot
        self.scheduler = AsyncIOScheduler(timezone=MSK)
        self.storage = DataStorage()
        self.setup_handlers()
        
    async def fetch_all_clubs_data(self) -> Dict[str, dict]:
        """Fetch current data for all clubs"""
        clubs_data = {}
        
        async with BrawlStarsAPI(BRAWLSTARS_API_KEY) as api:
            for club_name, club_tag in CLUBS.items():
                logger.info(f"Fetching data for {club_name}")
                club_info = await api.get_club_info(club_tag)
                
                if club_info:
                    clubs_data[club_name] = {
                        "trophies": club_info.get("trophies", 0),
                        "tag": club_tag,
                        "members": {}
                    }
                    
                    # Store member trophies
                    members = club_info.get("members", [])
                    for member in members:
                        clubs_data[club_name]["members"][member["tag"]] = {
                            "name": member["name"],
                            "trophies": member["trophies"]
                        }
                else:
                    logger.error(f"Failed to fetch data for {club_name}")
        
        return clubs_data
    
    def calculate_daily_changes(self, current_data: Dict, previous_data: Dict) -> Dict:
        """Calculate daily changes for clubs and players"""
        changes = {
            "clubs": {},
            "players": []
        }
        
        for club_name, club_data in current_data.items():
            current_trophies = club_data["trophies"]
            previous_trophies = previous_data.get(club_name, {}).get("trophies", current_trophies)
            
            trophy_change = StatsCalculator.calculate_trophy_change(
                current_trophies, previous_trophies
            )
            
            changes["clubs"][club_name] = {
                "trophies": current_trophies,
                "change": trophy_change
            }
            
            # Calculate player changes
            current_members = club_data.get("members", {})
            previous_members = previous_data.get(club_name, {}).get("members", {})
            
            for player_tag, player_data in current_members.items():
                current_player_trophies = player_data["trophies"]
                previous_player_trophies = previous_members.get(
                    player_tag, {}
                ).get("trophies", current_player_trophies)
                
                player_change = StatsCalculator.calculate_trophy_change(
                    current_player_trophies, previous_player_trophies
                )
                
                if player_change != 0:  # Only track players with changes
                    changes["players"].append(
                        (player_data["name"], club_name, player_change)
                    )
        
        return changes
    
    def format_daily_report(self, changes: Dict, date: datetime) -> str:
        """Форматировать ежедневный отчет статистики по клубам"""
        report = f"🏆 <b>Ежедневная статистика — {date.strftime('%d.%m.%Y')}</b>\n\n"
        
        # Сортировать клубы по изменению трофеев
        sorted_clubs = sorted(
            changes["clubs"].items(),
            key=lambda x: x[1]["change"],
            reverse=True
        )
        
        # Отчет для каждого клуба
        for club_name, data in sorted_clubs:
            change_str = StatsCalculator.format_trophy_change(data["change"])
            report += f"<b>{club_name}</b>\n"
            report += f"Трофеи: {data['trophies']} ({change_str} 🏆)\n"
            
            # Получить лучших игроков из этого клуба
            club_players = [(name, club, change) for name, club, change in changes["players"] if club == club_name]
            
            if club_players:
                top_club_players = StatsCalculator.get_top_players(club_players, 3)
                report += f"Лучшие игроки:\n"
                for i, (name, _, player_change) in enumerate(top_club_players, 1):
                    player_change_str = StatsCalculator.format_trophy_change(player_change)
                    report += f"  {i}. {name} — {player_change_str} 🏆\n"
            else:
                report += f"Нет изменений у игроков\n"
            
            report += "\n"
        
        return report
    
    def calculate_weekly_changes(self, current_data: Dict, weekly_data: Dict) -> Dict:
        """Calculate weekly changes"""
        changes = {
            "clubs": {},
            "players": []
        }
        
        for club_name, club_data in current_data.items():
            current_trophies = club_data["trophies"]
            weekly_start_trophies = weekly_data.get(club_name, {}).get("trophies", current_trophies)
            
            trophy_change = StatsCalculator.calculate_trophy_change(
                current_trophies, weekly_start_trophies
            )
            
            changes["clubs"][club_name] = {
                "trophies": current_trophies,
                "change": trophy_change
            }
            
            # Calculate player weekly changes
            current_members = club_data.get("members", {})
            weekly_members = weekly_data.get(club_name, {}).get("members", {})
            
            for player_tag, player_data in current_members.items():
                current_player_trophies = player_data["trophies"]
                weekly_player_trophies = weekly_members.get(
                    player_tag, {}
                ).get("trophies", current_player_trophies)
                
                player_change = StatsCalculator.calculate_trophy_change(
                    current_player_trophies, weekly_player_trophies
                )
                
                if player_change != 0:
                    changes["players"].append(
                        (player_data["name"], club_name, player_change)
                    )
        
        return changes
    
    def format_weekly_report(self, changes: Dict, date: datetime) -> str:
        """Форматировать еженедельный отчет статистики по клубам"""
        report = f"📅 <b>Еженедельный отчет — {date.strftime('%d.%m.%Y')}</b>\n\n"
        
        # Сортировать клубы по еженедельному изменению трофеев
        sorted_clubs = sorted(
            changes["clubs"].items(),
            key=lambda x: x[1]["change"],
            reverse=True
        )
        
        # Отчет для каждого клуба
        for club_name, data in sorted_clubs:
            change_str = StatsCalculator.format_trophy_change(data["change"])
            report += f"<b>{club_name}</b>\n"
            report += f"Трофеи: {data['trophies']} ({change_str} 🏆)\n"
            
            # Получить лучших игроков из этого клуба за неделю
            club_players = [(name, club, change) for name, club, change in changes["players"] if club == club_name]
            
            if club_players:
                top_club_players = StatsCalculator.get_top_players(club_players, 5)
                report += f"Лучшие игроки недели:\n"
                for i, (name, _, player_change) in enumerate(top_club_players, 1):
                    player_change_str = StatsCalculator.format_trophy_change(player_change)
                    report += f"  {i}. {name} — {player_change_str} 🏆\n"
            else:
                report += f"Нет изменений у игроков\n"
            
            report += "\n"
        
        return report
    
    def calculate_monthly_changes(self, current_data: Dict, monthly_data: Dict) -> Dict:
        """Calculate monthly changes"""
        changes = {
            "clubs": {},
            "players": []
        }
        
        for club_name, club_data in current_data.items():
            current_trophies = club_data["trophies"]
            monthly_start_trophies = monthly_data.get(club_name, {}).get("trophies", current_trophies)
            
            trophy_change = StatsCalculator.calculate_trophy_change(
                current_trophies, monthly_start_trophies
            )
            
            changes["clubs"][club_name] = {
                "trophies": current_trophies,
                "change": trophy_change
            }
            
            # Calculate player monthly changes
            current_members = club_data.get("members", {})
            monthly_members = monthly_data.get(club_name, {}).get("members", {})
            
            for player_tag, player_data in current_members.items():
                current_player_trophies = player_data["trophies"]
                monthly_player_trophies = monthly_members.get(
                    player_tag, {}
                ).get("trophies", current_player_trophies)
                
                player_change = StatsCalculator.calculate_trophy_change(
                    current_player_trophies, monthly_player_trophies
                )
                
                if player_change != 0:
                    changes["players"].append(
                        (player_data["name"], club_name, player_change)
                    )
        
        return changes
    
    def format_monthly_report(self, changes: Dict, date: datetime) -> str:
        """Форматировать ежемесячный отчет статистики по клубам"""
        report = f"📊 <b>Ежемесячный отчет — {date.strftime('%B %Y')}</b>\n\n"
        
        # Сортировать клубы по ежемесячному изменению трофеев
        sorted_clubs = sorted(
            changes["clubs"].items(),
            key=lambda x: x[1]["change"],
            reverse=True
        )
        
        # Отчет для каждого клуба
        for club_name, data in sorted_clubs:
            change_str = StatsCalculator.format_trophy_change(data["change"])
            trend_emoji = StatsCalculator.get_trend_emoji(data["change"])
            report += f"<b>{club_name}</b> {trend_emoji}\n"
            report += f"Трофеи: {data['trophies']} ({change_str} 🏆)\n"
            
            # Получить лучших игроков из этого клуба за месяц
            club_players = [(name, club, change) for name, club, change in changes["players"] if club == club_name]
            
            if club_players:
                top_club_players = StatsCalculator.get_top_players(club_players, 7)
                report += f"Топ игроки месяца:\n"
                for i, (name, _, player_change) in enumerate(top_club_players, 1):
                    player_change_str = StatsCalculator.format_trophy_change(player_change)
                    report += f"  {i}. {name} — {player_change_str} 🏆\n"
            else:
                report += f"Нет изменений у игроков\n"
            
            report += "\n"
        
        return report
    
    def get_all_players_with_trophies(self, current_data: Dict) -> List[Tuple[str, str, int]]:
        """Get all players with their current trophies"""
        players = []
        for club_name, club_data in current_data.items():
            members = club_data.get("members", {})
            for player_tag, player_data in members.items():
                players.append((player_data["name"], club_name, player_data["trophies"]))
        return players
    
    def format_top_players_report(self, current_data: Dict) -> str:
        """Форматировать отчет топ игроков по кубкам"""
        report = f"🏅 <b>Топ игроков по кубкам</b>\n\n"
        
        # Получить всех игрок��в с их кубками
        all_players = self.get_all_players_with_trophies(current_data)
        
        # Получить топ 15 игроков
        top_players = StatsCalculator.get_top_players_by_trophies(all_players, 15)
        
        for i, (name, club, trophies) in enumerate(top_players, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            report += f"{medal} <b>{name}</b> — {trophies} 🏆 ({club})\n"
        
        return report
    
    def format_top_gainers_report(self, changes: Dict, period: str = "день") -> str:
        """Форматировать отчет топ игроков по апу"""
        report = f"📈 <b>Топ игроки по апу за {period}</b>\n\n"
        
        # Получить топ 10 игроков по апу
        top_gainers = StatsCalculator.get_top_players(changes["players"], 10)
        
        for i, (name, club, change) in enumerate(top_gainers, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            change_str = StatsCalculator.format_trophy_change(change)
            report += f"{medal} <b>{name}</b> — {change_str} 🏆 ({club})\n"
        
        return report
    
    async def send_message(self, text: str):
        """Send message to Telegram chats"""
        for chat_id in CHAT_IDS:
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML'
                )
                logger.info(f"Message sent successfully to {chat_id}")
            except TelegramError as e:
                logger.error(f"Failed to send message to {chat_id}: {e}")
    
    async def daily_report_job(self):
        """Job to generate and send daily report"""
        logger.info("Running daily report job")
        
        try:
            # Fetch current data
            current_data = await self.fetch_all_clubs_data()
            
            if not current_data:
                logger.error("No data fetched, skipping report")
                return
            
            # Load previous data
            previous_data = self.storage.load_data(DAILY_DATA_FILE)
            
            # Calculate changes
            changes = self.calculate_daily_changes(current_data, previous_data)
            
            # Generate report
            now = datetime.now(MSK)
            report = self.format_daily_report(changes, now)
            
            # Send report
            await self.send_message(report)
            
            # Save current data for next day
            self.storage.save_data(DAILY_DATA_FILE, current_data)
            
        except Exception as e:
            logger.error(f"Error in daily report job: {e}")
    
    async def weekly_report_job(self):
        """Job to generate and send weekly report"""
        logger.info("Running weekly report job")
        
        try:
            # Fetch current data
            current_data = await self.fetch_all_clubs_data()
            
            if not current_data:
                logger.error("No data fetched, skipping weekly report")
                return
            
            # Load weekly start data
            weekly_data = self.storage.load_data(WEEKLY_DATA_FILE)
            
            # Calculate weekly changes
            changes = self.calculate_weekly_changes(current_data, weekly_data)
            
            # Generate report
            now = datetime.now(MSK)
            report = self.format_weekly_report(changes, now)
            
            # Send report
            await self.send_message(report)
            
            # Reset weekly data for next week
            self.storage.save_data(WEEKLY_DATA_FILE, current_data)
            
        except Exception as e:
            logger.error(f"Error in weekly report job: {e}")
    
    async def club_league_reminder(self):
        """Send Club League reminder"""
        message = "⚔️ <b>Club League Alert!</b>\n\nClub League starts in 30 minutes! Prepare your teams! 🎮"
        await self.send_message(message)
    
    async def power_league_reminder(self):
        """Send Power League end reminder"""
        message = "⏰ <b>Power League Alert!</b>\n\nOnly 2 hours left in this Power League season! Complete your matches! 💪"
        await self.send_message(message)
    
    async def setup_commands(self):
        """Setup bot commands menu"""
        commands = [
            BotCommand("start", "Start the bot"),
            BotCommand("daily", "Get daily statistics"),
            BotCommand("weekly", "Get weekly statistics"),
            BotCommand("help", "Show help message"),
        ]
        await self.bot.set_my_commands(commands)
        logger.info("Bot commands set")
    
    def setup_handlers(self):
        """Setup command handlers"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("daily", self.daily_command))
        self.application.add_handler(CommandHandler("weekly", self.weekly_command))
        self.application.add_handler(CommandHandler("monthly", self.monthly_command))
        self.application.add_handler(CommandHandler("top", self.top_command))
        self.application.add_handler(CommandHandler("gainers", self.gainers_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        logger.info("Command handlers registered")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /start"""
        try:
            await update.message.reply_text(
                "🤖 <b>Бот статистики Brawl Stars</b>\n\n"
                "Доступные команды:\n"
                "/daily - Получить ежедневную статистику\n"
                "/weekly - Получить еженедельную статистику\n"
                "/help - Показать это сообщение",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка в команде start: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="🤖 <b>Бот статистики Brawl Stars</b>\n\nДоступные команды:\n/daily - Получить ежедневную статистику\n/weekly - Получить еженедельную статистику\n/help - Показать это сообщение",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения: {e2}")
    
    async def daily_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /daily"""
        try:
            current_data = await self.fetch_all_clubs_data()
            if not current_data:
                await update.message.reply_text("❌ Ошибка получения данных")
                return
            
            previous_data = self.storage.load_data(DAILY_DATA_FILE)
            changes = self.calculate_daily_changes(current_data, previous_data)
            now = datetime.now(MSK)
            report = self.format_daily_report(changes, now)
            
            await update.message.reply_text(report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка в команде daily: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка: {str(e)}",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e2}")
    
    async def weekly_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /weekly"""
        try:
            current_data = await self.fetch_all_clubs_data()
            if not current_data:
                await update.message.reply_text("❌ Ошибка получения данных")
                return
            
            weekly_data = self.storage.load_data(WEEKLY_DATA_FILE)
            changes = self.calculate_weekly_changes(current_data, weekly_data)
            now = datetime.now(MSK)
            report = self.format_weekly_report(changes, now)
            
            await update.message.reply_text(report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка в команде weekly: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка: {str(e)}",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e2}")
    
    async def monthly_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /monthly"""
        try:
            current_data = await self.fetch_all_clubs_data()
            if not current_data:
                await update.message.reply_text("❌ Ошибка получения данных")
                return
            
            monthly_data = self.storage.load_data(MONTHLY_DATA_FILE)
            changes = self.calculate_monthly_changes(current_data, monthly_data)
            now = datetime.now(MSK)
            report = self.format_monthly_report(changes, now)
            
            await update.message.reply_text(report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка в команде monthly: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка: {str(e)}",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e2}")
    
    async def top_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /top - топ игроков по кубкам"""
        try:
            current_data = await self.fetch_all_clubs_data()
            if not current_data:
                await update.message.reply_text("❌ Ошибка получения данных")
                return
            
            report = self.format_top_players_report(current_data)
            await update.message.reply_text(report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка в команде top: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка: {str(e)}",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e2}")
    
    async def gainers_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /gainers - топ игроков по апу за день"""
        try:
            current_data = await self.fetch_all_clubs_data()
            if not current_data:
                await update.message.reply_text("❌ Ошибка получения данных")
                return
            
            previous_data = self.storage.load_data(DAILY_DATA_FILE)
            changes = self.calculate_daily_changes(current_data, previous_data)
            report = self.format_top_gainers_report(changes, "день")
            
            await update.message.reply_text(report, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Ошибка в команде gainers: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Ошибка: {str(e)}",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения об ошибке: {e2}")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработать команду /help"""
        try:
            await update.message.reply_text(
                "🤖 <b>Бот статистики Brawl Stars</b>\n\n"
                "<b>Доступные команды:</b>\n"
                "/daily - Ежедневная статистика\n"
                "/weekly - Еженедельная статистика\n"
                "/monthly - Ежемесячная статистика\n"
                "/top - Топ 15 игроков по кубкам\n"
                "/gainers - Топ 10 игроков по апу за день\n"
                "/help - Показать это сообщение",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка в команде help: {e}")
            try:
                await self.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="🤖 <b>Бот статистики Brawl Stars</b>\n\n<b>Доступные команды:</b>\n/daily - Ежедневная статистика\n/weekly - Еженедельная статистика\n/monthly - Ежемесячная статистика\n/top - Топ 15 игроков по кубкам\n/gainers - Топ 10 игроков по апу за день\n/help - Показать это сообщение",
                    parse_mode='HTML'
                )
            except Exception as e2:
                logger.error(f"Ошибка отправки сообщения: {e2}")
    
    def setup_jobs(self):
        """Setup all scheduled jobs"""
        # Daily report at 12:00 MSK
        self.scheduler.add_job(
            self.daily_report_job,
            CronTrigger(hour=12, minute=0, timezone=MSK),
            id='daily_report',
            replace_existing=True
        )
        
        # Weekly report every Sunday at 12:00 MSK
        self.scheduler.add_job(
            self.weekly_report_job,
            CronTrigger(day_of_week='sun', hour=12, minute=0, timezone=MSK),
            id='weekly_report',
            replace_existing=True
        )
        
        # Club League reminder - Fridays at 9:30 MSK (example time)
        self.scheduler.add_job(
            self.club_league_reminder,
            CronTrigger(day_of_week='fri', hour=9, minute=30, timezone=MSK),
            id='club_league_reminder',
            replace_existing=True
        )
        
        # Power League reminder - Last day of season at 10:00 MSK (adjust as needed)
        # This is an example - adjust based on actual Power League schedule
        self.scheduler.add_job(
            self.power_league_reminder,
            CronTrigger(day_of_week='mon', hour=10, minute=0, timezone=MSK),
            id='power_league_reminder',
            replace_existing=True
        )
        
        logger.info("All jobs scheduled successfully")
    
    async def initialize_data(self):
        """Initialize data files if they don't exist or are empty"""
        logger.info("Initializing data files")
        
        # Fetch initial data
        current_data = await self.fetch_all_clubs_data()
        
        if not current_data:
            logger.error("Failed to fetch initial data")
            return
        
        # Initialize daily data if not exists or is empty
        if not os.path.exists(DAILY_DATA_FILE) or os.path.getsize(DAILY_DATA_FILE) == 0:
            self.storage.save_data(DAILY_DATA_FILE, current_data)
            logger.info("Daily data file initialized")
        
        # Initialize weekly data if not exists or is empty
        if not os.path.exists(WEEKLY_DATA_FILE) or os.path.getsize(WEEKLY_DATA_FILE) == 0:
            self.storage.save_data(WEEKLY_DATA_FILE, current_data)
            logger.info("Weekly data file initialized")
    
    async def start(self):
        """Start the bot"""
        logger.info("Starting Brawl Stars Statistics Bot")
        
        # Initialize data
        await self.initialize_data()
        
        # Setup bot commands
        await self.setup_commands()
        
        # Setup scheduled jobs
        self.setup_jobs()
        
        # Start scheduler
        self.scheduler.start()
        logger.info("Scheduler started")
        
        # Send startup message
        await self.send_message("🤖 <b>Brawl Stars Statistics Bot is now online!</b>\n\nDaily reports will be posted at 12:00 MSK.")
        
        # Start polling with error handling
        async with self.application:
            await self.application.start()
            await self.application.updater.start_polling(allowed_updates=None)
            logger.info("Bot is polling for updates...")
            try:
                await asyncio.Event().wait()
            except KeyboardInterrupt:
                logger.info("Bot interrupted by user")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            finally:
                await self.application.updater.stop()
                await self.application.stop()
                logger.info("Bot stopped")


def main():
    """Main entry point"""
    bot = BrawlStarsBot()
    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
