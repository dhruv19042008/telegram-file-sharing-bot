from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramRetryAfter
)
import asyncio
import uuid
import json
import os
import logging
import aiosqlite
from datetime import datetime, timedelta
from time import time
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, CommandObject, Command
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, 
    KeyboardButton, FSInputFile, CallbackQuery, ReplyKeyboardRemove, 
    ChatJoinRequest, ErrorEvent
)
from aiogram.enums import ChatMemberStatus
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# ==========================================
#               CONFIGURATION               
# ==========================================
TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@your_channel")
PRIVATE_CHANNEL_ID = int(os.getenv("PRIVATE_CHANNEL_ID", "0"))
PRIVATE_CHANNEL_LINK = os.getenv("PRIVATE_CHANNEL_LINK", "https://t.me/+your_invite_link")
ADMIN_CONTACT_URL = os.getenv("ADMIN_CONTACT_URL", "https://t.me/your_admin_username")

# Image URLs shown in bot messages (replace with your own)
UNJOINED_PHOTO = os.getenv("UNJOINED_PHOTO", "https://example.com/unjoined.jpg")
JOINED_PHOTO = os.getenv("JOINED_PHOTO", "https://example.com/joined.jpg")
PREMIUM_BLOCKED_PHOTO = os.getenv("PREMIUM_BLOCKED_PHOTO", "https://example.com/premium_blocked.jpg")
LIMIT_REACHED_PHOTO = os.getenv("LIMIT_REACHED_PHOTO", "https://example.com/limit_reached.jpg")
PRICE_LIST_PHOTO = os.getenv("PRICE_LIST_PHOTO", "https://example.com/price_list.jpg")
# ==========================================

logging.basicConfig(level=logging.INFO)
if not TOKEN:
    raise SystemExit("Set the BOT_TOKEN environment variable before running.")
bot = Bot(token=TOKEN)
dp = Dispatcher()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Database and legacy migration files live next to this script
DB_FILE_SQL = os.path.join(BASE_DIR, "database.db")
OLD_DB_FILE = os.path.join(BASE_DIR, "files.json")
OLD_CREDITS_FILE = os.path.join(BASE_DIR, "credits.json")

# Dictionary to hold the queue locks for each user
user_file_locks = {}

# --- ASYNC SQLITE DATABASE HANDLING ---
async def init_and_migrate_db():
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        
        # 1. Create Tables
        await conn.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            daily_uses INTEGER DEFAULT 0,
            last_date TEXT,
            bought_credits TEXT DEFAULT '0',
            referred_by TEXT,
            referral_count INTEGER DEFAULT 0
        )''')
        
        # Safely add has_requested column to track join requests
        async with conn.execute("PRAGMA table_info(users)") as c:
            rows = await c.fetchall()
            columns = [row['name'] for row in rows]
            if 'has_requested' not in columns:
                await conn.execute("ALTER TABLE users ADD COLUMN has_requested INTEGER DEFAULT 0")
        
        await conn.execute('''CREATE TABLE IF NOT EXISTS files (
            unique_id TEXT PRIMARY KEY,
            file_data TEXT
        )''')
        
        # Create Table for Global Gift Links
        await conn.execute('''CREATE TABLE IF NOT EXISTS gift_links (
            code TEXT PRIMARY KEY,
            credits_amount INTEGER DEFAULT 10
        )''')
        
        # Create Table to track who claimed which gift so they can't double-claim
        await conn.execute('''CREATE TABLE IF NOT EXISTS gift_claims (
            code TEXT,
            user_id TEXT,
            PRIMARY KEY (code, user_id)
        )''')
        
        # Create Table for Multiple Admins
        await conn.execute('''CREATE TABLE IF NOT EXISTS admins (
            admin_id TEXT PRIMARY KEY
        )''')
        await conn.execute("INSERT OR IGNORE INTO admins (admin_id) VALUES (?)", (str(ADMIN_ID),))

        # 2. Migrate Old JSON Data (Runs only once)
        if os.path.exists(OLD_CREDITS_FILE):
            logging.info("Migrating credits.json to SQLite database...")
            try:
                with open(OLD_CREDITS_FILE, "r") as f:
                    old_credits = json.load(f)
                    for uid, data in old_credits.items():
                        await conn.execute('''INSERT OR IGNORE INTO users 
                            (user_id, daily_uses, last_date, bought_credits, referred_by, referral_count) 
                            VALUES (?, ?, ?, ?, ?, ?)''', 
                            (str(uid), data.get('daily_uses', 0), data.get('last_date', ''), 
                             str(data.get('bought_credits', 0)), data.get('referred_by'), data.get('referral_count', 0)))
                os.rename(OLD_CREDITS_FILE, OLD_CREDITS_FILE + ".bak") 
                logging.info("Credits migration successful!")
            except Exception as e: logging.error(f"Error migrating credits: {e}")

        if os.path.exists(OLD_DB_FILE):
            logging.info("Migrating files.json to SQLite database...")
            try:
                with open(OLD_DB_FILE, "r") as f:
                    old_files = json.load(f)
                    for fid, fdata in old_files.items():
                        await conn.execute('INSERT OR IGNORE INTO files (unique_id, file_data) VALUES (?, ?)', 
                                  (str(fid), json.dumps(fdata)))
                os.rename(OLD_DB_FILE, OLD_DB_FILE + ".bak") 
                logging.info("Files migration successful!")
            except Exception as e: logging.error(f"Error migrating files: {e}")

        await conn.commit()

# --- DATABASE HELPERS ---
async def get_user(user_id):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM users WHERE user_id = ?", (str(user_id).strip(),)) as c:
            row = await c.fetchone()
        return dict(row) if row else None

async def create_user(user_id, last_date):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute("INSERT OR IGNORE INTO users (user_id, last_date) VALUES (?, ?)", (str(user_id).strip(), last_date))
        await conn.commit()

async def update_user(user_id, **kwargs):
    if not kwargs: return
    columns = ", ".join([f"{k} = ?" for k in kwargs.keys()])
    values = list(kwargs.values()) + [str(user_id).strip()]
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute(f"UPDATE users SET {columns} WHERE user_id = ?", values)
        await conn.commit()

async def get_file(unique_id):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT file_data FROM files WHERE unique_id = ?", (str(unique_id).strip(),)) as c:
            row = await c.fetchone()
        return json.loads(row['file_data']) if row else None

async def save_file(unique_id, file_data):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute("INSERT OR REPLACE INTO files (unique_id, file_data) VALUES (?, ?)", (str(unique_id).strip(), json.dumps(file_data)))
        await conn.commit()

async def create_gift_link(code, amount=10):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute("INSERT INTO gift_links (code, credits_amount) VALUES (?, ?)", (str(code).strip(), amount))
        await conn.commit()

async def claim_gift_link(code, user_id):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        
        # 1. Check if the gift code actually exists
        async with conn.execute("SELECT credits_amount FROM gift_links WHERE code = ?", (str(code).strip(),)) as c:
            row = await c.fetchone()
            
        if not row:
            return "invalid", 0
            
        amount = row['credits_amount']
        
        # 2. Check if this specific user has already claimed this exact code
        async with conn.execute("SELECT 1 FROM gift_claims WHERE code = ? AND user_id = ?", (str(code).strip(), str(user_id).strip())) as c:
            claimed = await c.fetchone()
            
        if claimed:
            return "already_claimed", 0
            
        # 3. Mark it as claimed by this user
        await conn.execute("INSERT INTO gift_claims (code, user_id) VALUES (?, ?)", (str(code).strip(), str(user_id).strip()))
        await conn.commit()
        
        return "success", amount

# --- ADMIN DB HELPERS ---
async def is_admin(user_id):
    if str(user_id).strip() == str(ADMIN_ID).strip(): return True
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        async with conn.execute("SELECT 1 FROM admins WHERE admin_id = ?", (str(user_id).strip(),)) as c:
            row = await c.fetchone()
        return bool(row)

async def add_admin_db(user_id):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute("INSERT OR IGNORE INTO admins (admin_id) VALUES (?)", (str(user_id).strip(),))
        await conn.commit()

async def remove_admin_db(user_id):
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        await conn.execute("DELETE FROM admins WHERE admin_id = ?", (str(user_id).strip(),))
        await conn.commit()

async def get_all_admins():
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT admin_id FROM admins") as c:
            rows = await c.fetchall()
        return [row['admin_id'] for row in rows]

# --- UTILS & STATES ---
batch_mode = False
current_batch = []

broadcast_mode = False
broadcast_messages = []

# Premium upload modes (admin only)
prem_mode = False          # /prem  → single file, auto-generates link immediately
prem_batch_mode = False    # /prembatch → collect files, /done to finish
prem_batch_files = []

# Free upload mode (admin only)
free_batch_mode = False    # /freelink → collect files, /done to finish
free_batch_files = []

class SupportState(StatesGroup):
    waiting_for_message = State()

def get_main_menu():
    return ReplyKeyboardRemove()

async def delete_message_later(chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try: await bot.delete_message(chat_id, message_id)
    except: pass

async def check_unlimited(user_id):
    user_data = await get_user(user_id)
    if not user_data: return False, None
    
    val = str(user_data.get("bought_credits", "0"))
    if val == "Unlimited": return True, "♾ Lifetime"
    
    if val.startswith("UNT_"):
        try:
            expiry_str = val.replace("UNT_", "")
            expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d %H:%M:%S")
            if datetime.now() < expiry_dt: 
                return True, expiry_dt.strftime("%d %b %Y, %H:%M")
            else:
                await update_user(user_id, bought_credits="0")
                return False, None
        except: return False, None
    return False, None

def get_link_message(link: str) -> str:
    return (
        f"<blockquote>Hᴇʀᴇ's Yᴏᴜʀ Lɪɴᴋ 🔗\n"
        f"{link}</blockquote>\n\n"
        f"Rᴇᴀᴄᴛɪᴏɴ Dᴇᴅᴏ ⚡️\n\n"
        f"Join <a href='{PRIVATE_CHANNEL_LINK}'>backup Channel</a>"
    )

async def check_fsub(bot_instance: Bot, user_id: int) -> bool:
    if await is_admin(user_id): return True
    try:
        member1 = await bot_instance.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user_id)
        m1_joined = member1.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]
        
        m2_joined = False
        try:
            member2 = await bot_instance.get_chat_member(chat_id=PRIVATE_CHANNEL_ID, user_id=user_id)
            m2_joined = member2.status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]
        except Exception:
            pass
            
        user_record = await get_user(str(user_id))
        has_req = user_record.get('has_requested', 0) == 1 if user_record else False
        
        return m1_joined and (m2_joined or has_req)
    except Exception as e:
        logging.error(f"FSub check error: {e}")
        return False

# --- HANDLERS ---

@dp.chat_join_request()
async def handle_join_request(update: ChatJoinRequest):
    if str(update.chat.id) == str(PRIVATE_CHANNEL_ID):
        user_id = str(update.from_user.id)
        user_data = await get_user(user_id)
        if not user_data:
            await create_user(user_id, datetime.now().strftime("%Y-%m-%d"))
        # Give immediate access marker upon requesting
        await update_user(user_id, has_requested=1)

@dp.message(CommandStart())
async def start_command(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    args = command.args
    user_id = str(message.from_user.id)
    today = datetime.now().strftime("%Y-%m-%d")
    user_name = message.from_user.first_name

    # Ensure user exists in DB
    user_data = await get_user(user_id)
    if not user_data:
        await create_user(user_id, today)
        user_data = await get_user(user_id)

    # Define the two different photos
    unjoined_photo = UNJOINED_PHOTO
    joined_photo = JOINED_PHOTO

    unjoined_text = (
        f"👋 <b>Hello {user_name}!</b>\n\n"
        "🥺 <b>You need to join in my Channels to use me.</b>\n\n"
        "🔒 <i>Kindly Please join Channels to get exciting videos and features!</i> ✨"
    )

    if not args:
        is_subbed = await check_fsub(bot, message.from_user.id)
        
        # User has NOT joined
        if not is_subbed:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}")],
                [InlineKeyboardButton(text="🔒 Join Private Channel", url=PRIVATE_CHANNEL_LINK)]
            ])
            try:
                return await message.answer_photo(photo=unjoined_photo, caption=unjoined_text, reply_markup=keyboard, parse_mode="HTML")
            except TelegramForbiddenError:
                logging.info(f"User {user_id} has blocked the bot. Skipping.")
                return
        
        # User HAS joined
        else:
            joined_text = (
                f"👋 <b>Hello {user_name}!</b>\n\n"
                "📁 <i>I can store private files in Specified Channel and other users can access it from special link.</i> ✨"
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👑 Premium", callback_data="cb_premium"), InlineKeyboardButton(text="👨‍💻 Admin", url=ADMIN_CONTACT_URL)],
                [InlineKeyboardButton(text="📞 Support", callback_data="cb_support"), InlineKeyboardButton(text="🏆 Leaderboard", callback_data="cb_leaderboard")],
                [InlineKeyboardButton(text="👤 My Profile", callback_data="cb_profile"), InlineKeyboardButton(text="❌ Close", callback_data="cb_close")]
            ])
            try:
                return await message.answer_photo(photo=joined_photo, caption=joined_text, reply_markup=keyboard, parse_mode="HTML")
            except TelegramForbiddenError:
                logging.info(f"User {user_id} has blocked the bot. Skipping.")
                return

    # Gift Link Processing Logic 
    if args.startswith("gift_"):
        gift_code = args.replace("gift_", "")
        status, credits_to_add = await claim_gift_link(gift_code, user_id)
        
        if status == "invalid":
            return await message.answer("❌ <b>This gift link is invalid or does not exist.</b>", parse_mode="HTML")
        elif status == "already_claimed":
            return await message.answer("⚠️ <b>You have already claimed this gift!</b> It can only be used once per person.", parse_mode="HTML")
        
        try: current_credits = int(user_data.get("bought_credits", "0"))
        except: current_credits = 0
        
        if str(user_data.get("bought_credits")) != "Unlimited" and not str(user_data.get("bought_credits")).startswith("UNT_"):
            await update_user(user_id, bought_credits=str(current_credits + credits_to_add))
            
        await message.answer(
            f"🎉 <b>Success! You claimed +{credits_to_add} Premium Credits!</b>\n\n"
            "Your account balance has been updated. Open your profile to check.", 
            parse_mode="HTML"
        )
        return

    # Referral Logic
    if args.startswith("ref_"):
        referrer_id = args.replace("ref_", "")
        if user_id == referrer_id:
            return await message.answer("❌ Self-referral not allowed.")
        if user_data.get("referred_by"):
            return await message.answer("❌ Already referred.")
        
        is_subbed_ref = await check_fsub(bot, message.from_user.id)

        if not is_subbed_ref:
            bot_info = await bot.get_me()
            retry_link = f"https://t.me/{bot_info.username}?start=ref_{referrer_id}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}")],
                [InlineKeyboardButton(text="🔒 Join Private Channel", url=PRIVATE_CHANNEL_LINK)],
                [InlineKeyboardButton(text="🔄 Verify", url=retry_link)]
            ])
            return await message.answer_photo(
                photo=unjoined_photo,
                caption=unjoined_text,
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            
        referrer_data = await get_user(referrer_id)
        if referrer_data:
            is_unl, _ = await check_unlimited(referrer_id)
            if not is_unl:
                try: ref_credits = int(referrer_data["bought_credits"])
                except: ref_credits = 0
                await update_user(referrer_id, bought_credits=str(ref_credits + 1))
            
            await update_user(referrer_id, referral_count=referrer_data.get("referral_count", 0) + 1)
            await update_user(user_id, referred_by=referrer_id)
            await message.answer("✅ Referral success!")
            try: await bot.send_message(referrer_id, "🎉 <b>New Referral!</b>\n+1 Credit.", parse_mode="HTML")
            except: pass
        return

    # File Access Logic
    unique_id = args
    is_premium_link = unique_id.startswith("prem_")
    is_free_link = unique_id.startswith("free_")

    if not await is_admin(message.from_user.id):
        is_subbed_file = await check_fsub(bot, message.from_user.id)
            
        if not is_subbed_file:
            bot_info = await bot.get_me()
            retry_link = f"https://t.me/{bot_info.username}?start={unique_id}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Join Channel", url=f"https://t.me/{CHANNEL_USERNAME.replace('@', '')}")],
                [InlineKeyboardButton(text="🔒 Join Private Channel", url=PRIVATE_CHANNEL_LINK)],
                [InlineKeyboardButton(text="🔄 Get File", url=retry_link)]
            ])
            try:
                return await message.answer_photo(
                    photo=unjoined_photo,
                    caption=unjoined_text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )

            except TelegramForbiddenError:
                print(f"User {message.from_user.id} blocked the bot")
                return

            except Exception as e:
                print(f"Photo send error: {e}")
                return

    file_data = await get_file(unique_id)
    logging.info(f"LOOKUP: '{unique_id}' -> {file_data}")
    if not file_data:
        return await message.answer("❌ Invalid link.")

    # --- PREMIUM LINK GATE ---
    # If this is a premium link, verify the user has premium access BEFORE anything else
    if is_premium_link and not await is_admin(message.from_user.id):
        gate_user_data = await get_user(user_id)
        gate_is_unl, _ = await check_unlimited(user_id) if gate_user_data else (False, None)
        gate_credits = 0
        if gate_user_data:
            try: gate_credits = int(gate_user_data.get("bought_credits", "0"))
            except: gate_credits = 0

        if not gate_is_unl and gate_credits <= 0:
            prem_blocked_caption = (
                "👑 <b>PREMIUM CONTENT</b>\n\n"
                "🔒 <i>This content is for Premium Members only.</i>\n\n"
                "Upgrade your account to unlock exclusive premium files, "
                "or invite friends to earn free credits!\n\n"
                "⚡️ <b>WAYS TO GET ACCESS:</b>\n"
                "<blockquote>💳 Buy Premium — instant access</blockquote>\n"
                "<blockquote>👥 Invite friends & earn credits for free</blockquote>"
            )
            bot_info = await bot.get_me()
            ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Buy Premium", callback_data="show_upi")],
                [InlineKeyboardButton(text="👥 Invite Friend", switch_inline_query=f"\n{ref_link}")]
            ])
            return await message.answer_photo(
                photo=PREMIUM_BLOCKED_PHOTO,
                caption=prem_blocked_caption,
                reply_markup=kb,
                parse_mode="HTML"
            )

    if user_id not in user_file_locks:
        user_file_locks[user_id] = asyncio.Lock()

    try:
        async with user_file_locks[user_id]:
            user_data = await get_user(user_id)
            is_unl = False
            is_premium_access = False

            if not await is_admin(message.from_user.id):
                if user_data["last_date"] != today:
                    await update_user(user_id, daily_uses=0, last_date=today)
                    user_data["daily_uses"] = 0
                
                is_unl, _ = await check_unlimited(user_id)
                
                try: credits_int = int(user_data.get("bought_credits", "0"))
                except: credits_int = 0

                # Free links skip everything — no daily quota touched, no credits touched
                if is_free_link:
                    is_premium_access = True
                # Premium links skip the free daily quota — they always consume a credit
                elif is_premium_link:
                    if is_unl:
                        is_premium_access = True
                    elif credits_int > 0:
                        await update_user(user_id, bought_credits=str(credits_int - 1))
                        is_premium_access = True
                    # (non-premium already blocked above, so we never reach else here)
                elif user_data["daily_uses"] < 3:
                    await update_user(user_id, daily_uses=user_data["daily_uses"] + 1)
                    user_data["daily_uses"] += 1
                elif is_unl: 
                    is_premium_access = True
                elif credits_int > 0:
                    await update_user(user_id, bought_credits=str(credits_int - 1))
                    is_premium_access = True
                else:
                    limit_over_caption = (
                        "🛑 <b>DAILY FREE LIMIT REACHED!</b>\n"
                        "<i>Come back later or upgrade your account to continue.</i>\n\n"
                        "⚠️<b>WAYS TO GET PREMIUM</b> ⚠️\n\n"
                        "<blockquote>👥 Refer friends & earn Premium for free</blockquote>\n"
                        "<blockquote>💳 Pay with UPI</blockquote>"
                    )
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Pay with UPI", callback_data="show_upi")],
                        [InlineKeyboardButton(text="👥 Refer Friend", callback_data="show_referral")]
                    ])
                    return await message.answer_photo(
                        photo=LIMIT_REACHED_PHOTO, 
                        caption=limit_over_caption, 
                        reply_markup=kb, 
                        parse_mode="HTML"
                    )

            try:
                loading_msg = await message.answer("⏳ <b>Fetching media...</b>", parse_mode="HTML")
                await asyncio.sleep(0.8)
                await loading_msg.edit_text("🚀 <b>Preparing to send...</b>", parse_mode="HTML")
                await asyncio.sleep(0.8)
                await loading_msg.delete()
            except Exception:
                pass 

            files = [file_data] if isinstance(file_data, dict) else file_data
            current_uses = user_data.get("daily_uses", 0) if not await is_admin(message.from_user.id) else 0

            for f in files:
                try:
                    method = getattr(bot, f"send_{f['type']}")

                    sent = await method(
                        chat_id=message.chat.id,
                        **{f['type']: f['id']},
                        caption=f"📢 <b>Join {CHANNEL_USERNAME} & <a href='{PRIVATE_CHANNEL_LINK}'>Private Channel</a></b>\n\n⏳ <b>Auto-deleting in 10 mins.</b>",
                        parse_mode="HTML",
                        protect_content=True
                    )

                    asyncio.create_task(
                        delete_message_later(
                            message.chat.id,
                            sent.message_id,
                            600
                        )
                    )

                except TelegramForbiddenError:
                    logging.warning(
                        f"User {message.from_user.id} blocked bot"
                    )
                    continue

                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                    continue

                except Exception as e:
                    logging.error(
                        f"Send error: {e}"
                    )
                    continue

            # --- STATELESS SAVE BUTTON INTEGRATION ---
            flag = "1" if is_premium_access or await is_admin(message.from_user.id) else "0"
            cb_data = f"sv|{unique_id}|{user_id}|{flag}"
            save_btn = InlineKeyboardButton(text="💾 Save / Share Files", callback_data=cb_data)
                    
            if not await is_admin(message.from_user.id) and not is_unl and not is_free_link:
                try:
                    buy_kb = InlineKeyboardMarkup(inline_keyboard=[
                        [save_btn],
                        [InlineKeyboardButton(text="💎 Buy Premium", url=ADMIN_CONTACT_URL)]
                    ])
                    
                    remaining_uses = max(0, 3 - current_uses)
                    usage_msg = await message.answer(
                        f"<b>Free used: {current_uses}/3 | Remaining: {remaining_uses}</b>", 
                        parse_mode="HTML", 
                        reply_markup=buy_kb
                    )
                    asyncio.create_task(delete_message_later(message.chat.id, usage_msg.message_id, 600))
                except Exception as e:
                    logging.error(f"Usage message error: {e}")
            else:
                try:
                    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[save_btn]])
                    usage_msg = await message.answer(
                        "✅ <b>Media delivered successfully.</b>", 
                        parse_mode="HTML", 
                        reply_markup=admin_kb
                    )
                    asyncio.create_task(delete_message_later(message.chat.id, usage_msg.message_id, 600))
                except Exception as e:
                    logging.error(f"Admin/Unlimited usage message error: {e}")
    finally:
        # Delete the lock from memory once files are sent or returned to prevent memory leak
        if user_id in user_file_locks:
            del user_file_locks[user_id]

# --- CALLBACK HANDLERS FOR THE NEW MAIN MENU ---

@dp.callback_query(F.data.startswith("sv|"))
async def save_file_callback_handler(callback: CallbackQuery):
    parts = callback.data.split("|")

    if len(parts) != 4:
        return await callback.answer(
            "❌ Invalid button format.",
            show_alert=True
        )

    _, unique_id, buyer_id, premium_flag = parts
    is_premium_button = premium_flag == "1"

    clicker_id = str(callback.from_user.id)
    has_access = False
    
    if clicker_id == buyer_id and is_premium_button:
        has_access = True
    else:
        admin_status = await is_admin(clicker_id)
        user_data = await get_user(clicker_id)
        is_unl = False
        credits_int = 0
        
        if user_data:
            is_unl, _ = await check_unlimited(clicker_id)
            try: credits_int = int(user_data.get("bought_credits", "0"))
            except ValueError: credits_int = 0
            
        if admin_status or is_unl or credits_int > 0:
            has_access = True

    if not has_access:
        return await callback.answer(
            "🔒 This feature is for premium users only! Please upgrade to save or share files.", 
            show_alert=True
        )
        
    file_data = await get_file(unique_id)
    if not file_data:
        return await callback.answer(f"❌ File ID [{unique_id}] not found.", show_alert=True)
        
    await callback.answer("✅ Premium feature unlocked! Sending files...", show_alert=False)

    files = [file_data] if isinstance(file_data, dict) else file_data

    for f in files:
        try:
            method = getattr(bot, f"send_{f['type']}")

            await method(
                chat_id=callback.message.chat.id,
                **{f['type']: f['id']},
                caption="🔓 <b>Unlocked File</b>\n<i>You can now save or forward this to anyone.</i>",
                parse_mode="HTML",
                protect_content=False
            )

            await asyncio.sleep(0.3)

        except TelegramForbiddenError:
            logging.warning(
                f"User {callback.from_user.id} blocked bot"
            )
            continue

        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            continue

        except Exception as e:
            logging.error(
                f"Save file send error: {e}"
            )
            continue

@dp.callback_query(F.data == "cb_premium")
async def cb_premium_handler(callback: CallbackQuery):
    price_chart = (
        "💎 <b>PREMIUM ACCESS</b>\n\n"
        "Contact the admin to upgrade your account."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👨‍💻 Contact Admin", url=ADMIN_CONTACT_URL)]
    ])
    await callback.message.answer_photo(
        photo=PRICE_LIST_PHOTO, 
        caption=price_chart, 
        reply_markup=kb, 
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "cb_support")
async def cb_support_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "✍️ <b>Support Desk</b>\n\n"
        "Please send your message, question, or issue below. The admin will review it as soon as possible.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel Support")]], resize_keyboard=True)
    )
    await state.set_state(SupportState.waiting_for_message)
    await callback.answer()

@dp.callback_query(F.data == "cb_leaderboard")
async def cb_leaderboard_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    
    # Safely fetch only users with at least 1 referral
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT user_id, referral_count FROM users WHERE user_id != ? AND referral_count > 0 ORDER BY referral_count DESC LIMIT 10", 
            (str(ADMIN_ID),)
        ) as c:
            top_users = await c.fetchall()

    # If the list is empty, nobody has referrals yet
    if not top_users: 
        await callback.message.answer("No referrers yet!")
        return await callback.answer()
        
    text = "🏆 <b>TOP REFERRERS</b>\n━━━━━━━━━━━━━━\n"
    for i, row in enumerate(top_users):
        text += f"{i+1}. <code>{row['user_id']}</code> — <b>{row['referral_count']}</b> invites\n"
        
    await callback.message.answer(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "cb_profile")
async def cb_profile_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = str(callback.from_user.id)
    user_data = await get_user(user_id)
    if not user_data:
        await create_user(user_id, datetime.now().strftime("%Y-%m-%d"))
        user_data = await get_user(user_id)
    
    is_unl, expiry = await check_unlimited(user_id)
    status = f"💎 {user_data.get('bought_credits', 0)} Credits"
    if is_unl: status = f"♾️ {expiry}"
    
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    text = (
        f"👤 <b>PROFILE</b>\n"
        f"🆔: <code>{user_id}</code>\n"
        f"📅 Free Today: {max(0, 3-user_data.get('daily_uses',0))}/3\n"
        f"💳 Status: <b>{status}</b>\n"
        f"👥 Invites: {user_data.get('referral_count', 0)}\n\n"
        f"🔗 Invite Link:\n<code>{ref_link}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Share", switch_inline_query=f"\n{ref_link}")]])
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "cb_close")
async def cb_close_handler(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()


# --- CALLBACK HANDLERS FOR LIMIT REACHED BUTTONS ---

@dp.callback_query(F.data == "show_upi")
async def upi_callback_handler(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except Exception:
        pass
    price_chart = (
        "💎 <b>PREMIUM ACCESS</b>\n\n"
        "Contact the admin to upgrade your account."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Contact Admin to Buy", url=ADMIN_CONTACT_URL)]
    ])
    await callback.message.answer_photo(
        photo=PRICE_LIST_PHOTO, 
        caption=price_chart, 
        reply_markup=kb, 
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "show_referral")
async def referral_callback_handler(callback: CallbackQuery):
    
    # 2. Answer the callback IMMEDIATELY to prevent timeouts
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            print("Ignored an expired callback query.")
            return # Stop processing if it's too old
        else:
            raise

    # 3. Proceed with the rest of your logic
    try:
        await callback.message.delete()
    except Exception:
        pass
        
    user_id = str(callback.from_user.id)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    
    fancy_text = (
        "🎁 <b>UNLOCK PREMIUM FOR FREE!</b> 🎁\n\n"
        "Don't want to pay? No problem! Invite your friends and earn premium credits instantly.\n\n"
        "✨ <b>1 Friend Joined = 1 Premium Credit</b>\n\n"
        "👇 <b>Your Exclusive Invite Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        "<i>Tap the button below to share it directly with your contacts!</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Share Link", switch_inline_query=f"\n{ref_link}")]
    ])
    
    await callback.message.answer(text=fancy_text, reply_markup=kb, parse_mode="HTML")


# --- OTHER COMMANDS (Kept for backwards compatibility) ---
@dp.message(F.text == "👤 My Profile")
@dp.message(Command("profile"))
async def profile_command(message: Message, state: FSMContext):
    await state.clear()
    user_id = str(message.from_user.id)
    user_data = await get_user(user_id)
    if not user_data:
        await create_user(user_id, datetime.now().strftime("%Y-%m-%d"))
        user_data = await get_user(user_id)
    
    is_unl, expiry = await check_unlimited(user_id)
    status = f"💎 {user_data.get('bought_credits', 0)} Credits"
    if is_unl: status = f"♾️ {expiry}"
    
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    text = (
        f"👤 <b>PROFILE</b>\n"
        f"🆔: <code>{user_id}</code>\n"
        f"📅 Free Today: {max(0, 3-user_data.get('daily_uses',0))}/3\n"
        f"💳 Status: <b>{status}</b>\n"
        f"👥 Invites: {user_data.get('referral_count', 0)}\n\n"
        f"🔗 Invite Link:\n<code>{ref_link}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Share", switch_inline_query=f"\n{ref_link}")]])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "🏆 Leaderboard")
async def leaderboard_command(message: Message, state: FSMContext):
    await state.clear()
    
    # Safely fetch only users with at least 1 referral
    async with aiosqlite.connect(DB_FILE_SQL) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT user_id, referral_count FROM users WHERE user_id != ? AND referral_count > 0 ORDER BY referral_count DESC LIMIT 10", 
            (str(ADMIN_ID),)
        ) as c:
            top_users = await c.fetchall()

    # If the list is empty, nobody has referrals yet
    if not top_users: 
        return await message.answer("No referrers yet!")
        
    text = "🏆 <b>TOP REFERRERS</b>\n━━━━━━━━━━━━━━\n"
    for i, row in enumerate(top_users):
        text += f"{i+1}. <code>{row['user_id']}</code> — <b>{row['referral_count']}</b> invites\n"
        
    await message.answer(text, parse_mode="HTML")


# --- SUPPORT HANDLERS ---
@dp.message(F.text == "📞 Support")
async def support_button_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "✍️ <b>Support Desk</b>\n\n"
        "Please send your message, question, or issue below. The admin will review it as soon as possible.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Cancel Support")]], resize_keyboard=True)
    )
    await state.set_state(SupportState.waiting_for_message)

@dp.message(F.text == "❌ Cancel Support")
async def cancel_support_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Support request cancelled. Send /start to open the menu.", reply_markup=get_main_menu())

@dp.message(SupportState.waiting_for_message)
async def receive_support_message(message: Message, state: FSMContext):
    user_info = f"<code>{message.from_user.id}</code>"
    if message.from_user.username:
        user_info += f" (@{message.from_user.username})"
        
    await bot.send_message(
        chat_id=ADMIN_ID, 
        text=f"📞 <b>New Support Message</b>\nFrom: {user_info}", 
        parse_mode="HTML"
    )
    
    await message.copy_to(chat_id=ADMIN_ID)
    await message.answer("✅ <b>Message Sent!</b>\nThe admin has received your message and will review it.", parse_mode="HTML", reply_markup=get_main_menu())
    await state.clear()


# --- ADMIN FUNCTIONS ---

@dp.message(F.text.lower().startswith("/makegift"))
async def make_gift_command(message: Message):
    if not await is_admin(message.from_user.id): return
    
    amount = 10
    parts = message.text.split()
    
    if len(parts) > 1:
        try:
            amount = int(parts[1])
            if amount <= 0:
                return await message.reply("❌ Credit amount must be greater than 0.")
        except ValueError:
            return await message.reply("⚠️ Usage: <code>/makegift [amount]</code>\nExample: <code>/makegift 50</code>", parse_mode="HTML")

    gift_code = str(uuid.uuid4())[:8]
    await create_gift_link(gift_code, amount=amount)
    
    bot_info = await bot.get_me()
    gift_url = f"https://t.me/{bot_info.username}?start=gift_{gift_code}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Claim Premium Credits 🎁", url=gift_url)]
    ])
    
    fancy_announcement = (
        "⚡️ <b>MASSIVE PREMIUM GIVEAWAY!</b> ⚡️\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🎁 I'm giving away <b>{amount} Premium Credits</b> to EVERYONE!\n\n"
        "👇 <i>Tap the button below to claim your free credits!</i>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ <b>Note:</b> Every user can claim this link exactly <b>1 time</b>."
    )
    
    await message.answer(fancy_announcement, reply_markup=keyboard, parse_mode="HTML")

@dp.message(Command("backup"))
async def backup_database(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        if os.path.exists(DB_FILE_SQL):
            db_file = FSInputFile(DB_FILE_SQL)
            await message.reply_document(
                document=db_file, 
                caption="📦 <b>Database Backup</b>\nThis file contains all your saved links, user data, and referral counts up to this exact moment.", 
                parse_mode="HTML"
            )
        else:
            await message.reply("❌ Database file not found!")
    except Exception as e:
        await message.reply(f"❌ Failed to backup database: {e}")

@dp.message(Command("broadcast"))
async def start_broadcast(message: Message):
    global broadcast_mode, broadcast_messages, batch_mode, prem_mode, prem_batch_mode, prem_batch_files, free_batch_mode, free_batch_files
    if not await is_admin(message.from_user.id): return
    
    batch_mode = False 
    prem_mode = False
    prem_batch_mode = False
    prem_batch_files = []
    free_batch_mode = False
    free_batch_files = []
    broadcast_mode = True
    broadcast_messages = []
    
    await message.reply(
        "📡 <b>Broadcast Setup Started!</b>\n"
        "Send me everything you want to broadcast (text, images, albums, files, etc.).\n"
        "When you are finished sending, type <code>/done</code>", 
        parse_mode="HTML"
    )

@dp.message(lambda msg: msg.text and msg.text.lower().startswith("set lifetime"))
async def set_lifetime_command(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        clean_text = message.text.lower().replace("set lifetime", "").replace("[", "").replace("]", "").strip()
        
        if not clean_text:
            return await message.reply("⚠️ Usage: <code>set lifetime user_id</code>", parse_mode="HTML")
        
        tid = clean_text 
        
        if not await get_user(tid): await create_user(tid, datetime.now().strftime("%Y-%m-%d"))
        await update_user(tid, bought_credits="Unlimited")
        
        await message.reply(f"✅ Lifetime activated for <code>{tid}</code>", parse_mode="HTML")
        try: await bot.send_message(tid, "🚀 <b>Lifetime Unlimited Access Activated!</b>", parse_mode="HTML")
        except Exception: pass 
            
    except Exception as e: 
        await message.reply(f"❌ <b>Code Error:</b> {e}")

@dp.message(lambda msg: msg.text and msg.text.lower().startswith("set timed"))
async def set_timed_command(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        clean_text = message.text.lower().replace("set timed", "").replace("[", "").replace("]", "").strip()
        args = clean_text.split()
        
        if len(args) < 2: 
            return await message.reply("⚠️ Usage: <code>set timed user_id days</code>", parse_mode="HTML")
        
        tid = args[0]
        days = int(args[1])
        
        expiry_dt = datetime.now() + timedelta(days=days)
        expiry_str = expiry_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        if not await get_user(tid): await create_user(tid, datetime.now().strftime("%Y-%m-%d"))
        await update_user(tid, bought_credits=f"UNT_{expiry_str}")
        
        await message.reply(f"✅ {days} days unlimited activated for <code>{tid}</code>", parse_mode="HTML")
        try: await bot.send_message(tid, f"🚀 <b>Unlimited Access Activated for {days} days!</b>\nExpires: {expiry_str}", parse_mode="HTML")
        except Exception: pass
    except Exception as e: 
        await message.reply(f"❌ <b>Code Error:</b> {e}")

@dp.message(lambda msg: msg.text and msg.text.lower().startswith("add credit"))
async def add_credits_command(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        clean_text = message.text.lower().replace("add credit", "").replace("[", "").replace("]", "").strip()
        args = clean_text.split()
        
        if len(args) < 2: 
            return await message.reply("⚠️ Usage: <code>add credit user_id amount</code>", parse_mode="HTML")
            
        tid = args[0]
        amt = int(args[1])
        
        user_data = await get_user(tid)
        if not user_data: 
            await create_user(tid, datetime.now().strftime("%Y-%m-%d"))
            user_data = await get_user(tid)
            
        val_str = str(user_data.get("bought_credits", "0"))
        if val_str == "Unlimited" or val_str.startswith("UNT_"):
            await update_user(tid, bought_credits=str(amt))
        else:
            try: current_amt = int(val_str)
            except: current_amt = 0
            await update_user(tid, bought_credits=str(current_amt + amt))
            
        await message.reply(f"✅ Added {amt} credits to <code>{tid}</code>", parse_mode="HTML")
        try: await bot.send_message(tid, f"💎 <b>Credits Added!</b>\nAdmin added <b>{amt}</b> credits.", parse_mode="HTML")
        except Exception: pass
    except Exception as e: 
        await message.reply(f"❌ <b>Code Error:</b> {e}")

@dp.message(lambda msg: msg.text and msg.text.lower().startswith("check credit"))
async def check_credits_command(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        clean_text = message.text.lower().replace("check credit", "").replace("[", "").replace("]", "").strip()
        if not clean_text:
            return await message.reply("⚠️ Usage: <code>check credit user_id</code>", parse_mode="HTML")
            
        tid = clean_text
        
        user_data = await get_user(tid)
        if not user_data: 
            return await message.reply(f"❌ User <code>{tid}</code> not found in database.", parse_mode="HTML")
            
        is_unl, expiry = await check_unlimited(tid)
        if is_unl: status = f"♾️ {expiry}"
        else: status = f"💎 {user_data.get('bought_credits', '0')} Credits"
            
        await message.reply(
            f"👤 <b>User Info:</b> <code>{tid}</code>\n"
            f"💳 <b>Status:</b> {status}\n"
            f"👥 <b>Referrals:</b> {user_data.get('referral_count', 0)}\n"
            f"⏳ <b>Free Used Today:</b> {user_data.get('daily_uses', 0)}/3",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.reply(f"❌ <b>Code Error:</b> {e}")

@dp.message(lambda msg: msg.text and msg.text.lower().startswith("reset user"))
async def reset_user_command(message: Message):
    if not await is_admin(message.from_user.id): return
    try:
        clean_text = message.text.lower().replace("reset user", "").replace("[", "").replace("]", "").strip()
        if not clean_text:
            return await message.reply("⚠️ Usage: <code>reset user user_id</code>", parse_mode="HTML")
            
        tid = clean_text
        
        user_data = await get_user(tid)
        if not user_data: 
            return await message.reply(f"❌ User <code>{tid}</code> not found in database.", parse_mode="HTML")
            
        await update_user(
            tid, bought_credits="0", daily_uses=0, referral_count=0, 
            referred_by=None, last_date=datetime.now().strftime("%Y-%m-%d")
        )
        
        await message.reply(f"✅ User <code>{tid}</code> has been completely reset.", parse_mode="HTML")
        try: await bot.send_message(tid, "🔄 <b>Your account limits and status have been reset by the Admin.</b>", parse_mode="HTML")
        except Exception: pass 
            
    except Exception as e:
        await message.reply(f"❌ <b>Code Error:</b> {e}")


@dp.message(Command("prem"))
async def start_prem_single(message: Message):
    global prem_mode, prem_batch_mode, prem_batch_files, batch_mode, broadcast_mode, free_batch_mode, free_batch_files
    if not await is_admin(message.from_user.id): return
    # Reset all other modes
    batch_mode = False
    broadcast_mode = False
    prem_batch_mode = False
    prem_batch_files = []
    free_batch_mode = False
    free_batch_files = []
    prem_mode = True
    await message.reply(
        "👑 <b>Premium Single-File Mode ON</b>\n\n"
        "Send one file and I'll generate a <b>premium-only link</b> instantly.\n"
        "Only users with credits, timed access, or lifetime unlimited can open it.",
        parse_mode="HTML"
    )

@dp.message(Command("prembatch"))
async def start_prem_batch(message: Message):
    global prem_mode, prem_batch_mode, prem_batch_files, batch_mode, broadcast_mode, free_batch_mode, free_batch_files
    if not await is_admin(message.from_user.id): return
    # Reset all other modes
    batch_mode = False
    broadcast_mode = False
    prem_mode = False
    free_batch_mode = False
    free_batch_files = []
    prem_batch_mode = True
    prem_batch_files = []
    await message.reply(
        "👑 <b>Premium Batch Mode ON</b>\n\n"
        "Send multiple files now. When done, type <code>/done</code> to generate a single <b>premium-only link</b>.",
        parse_mode="HTML"
    )

@dp.message(Command("freelink"))
async def start_free_batch(message: Message):
    global free_batch_mode, free_batch_files, batch_mode, broadcast_mode, prem_mode, prem_batch_mode, prem_batch_files
    if not await is_admin(message.from_user.id): return
    # Reset all other modes
    batch_mode = False
    broadcast_mode = False
    prem_mode = False
    prem_batch_mode = False
    prem_batch_files = []
    free_batch_mode = True
    free_batch_files = []
    await message.reply(
        "🆓 <b>Free Link Mode ON</b>\n\n"
        "Send one or more files now. When done, type <code>/done</code> to generate a link "
        "that <b>never consumes free daily uses or premium credits</b> for anyone who opens it.",
        parse_mode="HTML"
    )

@dp.message(Command("batch"))
async def start_batch(message: Message):
    global batch_mode, current_batch, broadcast_mode, prem_mode, prem_batch_mode, prem_batch_files, free_batch_mode, free_batch_files
    if await is_admin(message.from_user.id):
        broadcast_mode = False
        prem_mode = False
        prem_batch_mode = False
        prem_batch_files = []
        free_batch_mode = False
        free_batch_files = []
        batch_mode, current_batch = True, []
        await message.reply("📦 Batch Mode Started! Send all files and then type /done")

@dp.message(Command("done"))
async def finish_action(message: Message):
    global batch_mode, current_batch, broadcast_mode, broadcast_messages, prem_batch_mode, prem_batch_files, prem_mode, free_batch_mode, free_batch_files
    if not await is_admin(message.from_user.id): return

    # --- IF FINISHING A FREE BATCH ---
    if free_batch_mode:
        if not free_batch_files:
            free_batch_mode = False
            return await message.reply("❌ No files in free batch.")
        uid = "free_" + str(uuid.uuid4())[:8]
        await save_file(uid, free_batch_files)
        free_batch_mode = False
        free_batch_files = []
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start={uid}"
        await message.reply(
            f"🆓 <b>Free Link Generated!</b>\n\n"
            f"🔗 <code>{link}</code>\n\n"
            f"<i>Anyone who opens this will NOT lose any free daily uses or premium credits.</i>",
            parse_mode="HTML"
        )
        return

    # --- IF FINISHING A PREMIUM BATCH ---
    if prem_batch_mode:
        if not prem_batch_files:
            prem_batch_mode = False
            return await message.reply("❌ No files in premium batch.")
        uid = "prem_" + str(uuid.uuid4())[:8]
        await save_file(uid, prem_batch_files)
        prem_batch_mode = False
        prem_batch_files = []
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start={uid}"
        await message.reply(
            f"👑 <b>Premium Batch Link Generated!</b>\n\n"
            f"🔗 <code>{link}</code>\n\n"
            f"<i>Only premium users (credits / timed / lifetime) can access this.</i>",
            parse_mode="HTML"
        )
        return

    # --- IF FINISHING A NORMAL FILE BATCH ---
    if batch_mode:
        if not current_batch:
            return await message.reply("❌ No files in batch.")
        uid = str(uuid.uuid4())[:8]
        await save_file(uid, current_batch)
        batch_mode = False
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start={uid}"
        await message.reply(get_link_message(link), parse_mode="HTML")
        
    # --- IF FINISHING A BROADCAST ---
    elif broadcast_mode:
        if not broadcast_messages:
            broadcast_mode = False
            return await message.reply("❌ No messages were collected. Broadcast cancelled.")
            
        broadcast_mode = False
        
        async with aiosqlite.connect(DB_FILE_SQL) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute("SELECT user_id FROM users") as c:
                rows = await c.fetchall()
                all_users = [row['user_id'] for row in rows]

        sent, failed = 0, 0
        progress = await message.answer(f"⏳ Broadcasting {len(broadcast_messages)} messages to {len(all_users)} users...")
        
        for uid in all_users:
            user_success = False
            for msg_id in broadcast_messages:
                try:
                    await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=msg_id)
                    user_success = True
                    await asyncio.sleep(0.05)
                except TelegramForbiddenError:
                    logging.info(f"Broadcast: user {uid} has blocked the bot. Skipping.")
                    break  # No point trying other messages for this user
                except TelegramRetryAfter as e:
                    logging.info(f"Rate limited by Telegram. Sleeping for {e.retry_after} seconds...")
                    await asyncio.sleep(e.retry_after)
                    try:
                        await bot.copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=msg_id)
                        user_success = True
                    except TelegramForbiddenError:
                        logging.info(f"Broadcast: user {uid} blocked bot on retry. Skipping.")
                        break
                    except Exception:
                        pass
                except Exception:
                    pass
                    
            if user_success:
                sent += 1
            else:
                failed += 1
                
        await progress.edit_text(f"✅ <b>Broadcast Complete</b>\n🚀 Reached: {sent} users\n❌ Failed: {failed}", parse_mode="HTML")
    
    else:
        await message.reply("⚠️ You are not in Batch Mode or Broadcast Mode.")

# --- NEW ADMIN LIST, ADD, AND REMOVE HANDLERS (SLASH COMMANDS) ---

@dp.message(Command("addadmin", "add_admin"))
async def add_admin_command(message: Message, command: CommandObject):
    # Only the primary OWNER can add new admins
    if str(message.from_user.id).strip() != str(ADMIN_ID).strip(): 
        return await message.reply("❌ <b>Error:</b> Only the Main Owner can add new admins.", parse_mode="HTML")
        
    if not command.args:
        return await message.reply("⚠️ Usage: <code>/addadmin [user_id]</code>", parse_mode="HTML")
        
    try:
        new_admin = command.args.strip()
        await add_admin_db(new_admin)
        await message.reply(f"✅ User <code>{new_admin}</code> has been successfully added as an Admin.", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Add admin error: {e}")

@dp.message(Command("removeadmin", "remove_admin"))
async def remove_admin_command(message: Message, command: CommandObject):
    # Only the primary OWNER can remove admins
    if str(message.from_user.id).strip() != str(ADMIN_ID).strip(): 
        return await message.reply("❌ <b>Error:</b> Only the Main Owner can remove admins.", parse_mode="HTML")
        
    if not command.args:
        return await message.reply("⚠️ Usage: <code>/removeadmin [user_id]</code>", parse_mode="HTML")
        
    try:
        old_admin = command.args.strip()
        
        if old_admin == str(ADMIN_ID).strip():
            return await message.reply("❌ You cannot remove the main Owner.", parse_mode="HTML")
            
        await remove_admin_db(old_admin)
        await message.reply(f"✅ User <code>{old_admin}</code> has been removed from Admins.", parse_mode="HTML")
    except Exception as e:
        logging.error(f"Remove admin error: {e}")

@dp.message(Command("adminlist", "admin_list"))
async def admin_list_command(message: Message):
    # Only the primary OWNER can check the admin list
    if str(message.from_user.id).strip() != str(ADMIN_ID).strip(): 
        return await message.reply("❌ <b>Error:</b> Only the Main Owner can view the admin list.", parse_mode="HTML")
        
    try:
        admins = await get_all_admins()
        text = "👨‍💻 <b>ADMIN LIST:</b>\n━━━━━━━━━━━━━━\n"
        for adm in admins:
            clean_adm = str(adm).strip()
            if clean_adm == str(ADMIN_ID).strip():
                text += f"👑 <code>{clean_adm}</code> (Owner)\n"
            else:
                text += f"👤 <code>{clean_adm}</code>\n"
        await message.reply(text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Admin list error: {e}")

@dp.message(F.content_type.in_({'photo', 'video', 'document', 'animation'}))
async def handle_uploads(message: Message):
    global batch_mode, current_batch, broadcast_mode, broadcast_messages, prem_mode, prem_batch_mode, prem_batch_files, free_batch_mode, free_batch_files
    if not await is_admin(message.from_user.id): return
    
    if message.text and message.text.strip().startswith("/"): return
    
    if broadcast_mode:
        broadcast_messages.append(message.message_id)
        return
        
    f_id, f_type = None, None
    if message.photo: f_id, f_type = message.photo[-1].file_id, "photo"
    elif message.video: f_id, f_type = message.video.file_id, "video"
    elif message.document: f_id, f_type = message.document.file_id, "document"
    elif message.animation: f_id, f_type = message.animation.file_id, "animation"
    
    if not f_id: return
    f_data = {"id": f_id, "type": f_type}

    # --- FREE BATCH MODE: collect files, generate link on /done ---
    if free_batch_mode:
        free_batch_files.append(f_data)
        await message.reply(f"🆓 Free Added ({len(free_batch_files)}) — type /done when finished.")
        return

    # --- PREMIUM BATCH MODE: collect files, generate link on /done ---
    if prem_batch_mode:
        prem_batch_files.append(f_data)
        await message.reply(f"👑 Premium Added ({len(prem_batch_files)}) — type /done when finished.")
        return

    # --- PREMIUM SINGLE MODE: generate premium link immediately ---
    if prem_mode:
        prem_mode = False
        uid = "prem_" + str(uuid.uuid4())[:8]
        await save_file(uid, f_data)
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start={uid}"
        await message.reply(
            f"👑 <b>Premium Link Generated!</b>\n\n"
            f"🔗 <code>{link}</code>\n\n"
            f"<i>Only premium users (credits / timed / lifetime) can access this.</i>",
            parse_mode="HTML"
        )
        return

    # --- NORMAL BATCH MODE ---
    if batch_mode:
        current_batch.append(f_data)
        await message.reply(f"➕ Added ({len(current_batch)})")
    else:
        # --- NORMAL SINGLE FILE ---
        uid = str(uuid.uuid4())[:8]
        await save_file(uid, f_data)
        bot_info = await bot.get_me()
        link = f"https://t.me/{bot_info.username}?start={uid}"
        await message.reply(get_link_message(link), parse_mode="HTML")

# --- GLOBAL ERROR HANDLER ---
@dp.error()
async def global_error_handler(event: ErrorEvent):
    try:
        if isinstance(event.exception, TelegramForbiddenError):
            # User blocked the bot — log quietly, do NOT crash other users
            logging.info(f"Bot was blocked by user. Update ID: {event.update.update_id}")
        else:
            logging.error(
                f"Update ID: {event.update.update_id}\n"
                f"Exception: {repr(event.exception)}",
                exc_info=True
            )
    except Exception:
        pass

    return True  # Returning True tells aiogram: error handled, do NOT re-raise


async def main():
    logging.info(f"DB PATH EXISTS: {os.path.exists(DB_FILE_SQL)}")
    await init_and_migrate_db()

    while True:
        try:
            await dp.start_polling(
                bot,
                allowed_updates=[
                    "message",
                    "callback_query",
                    "chat_join_request"
                ]
            )

        except (KeyboardInterrupt, SystemExit):
            logging.info("Bot stopped by user!")
            await bot.session.close()
            return

        except Exception as e:
            logging.error(
                f"Polling crashed: {e}",
                exc_info=True
            )

            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped by user!")
    except Exception as e:
        logging.critical(f"Fatal error in main execution: {e}", exc_info=True)
