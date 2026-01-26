import os
import json
import asyncio
import asyncpg
from urllib.parse import urlparse
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import aiohttp
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

# Загружаем .env файл (для локального запуска)
load_dotenv()

# ==================== КОНФИГИ ИЗ ENV ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "Inkonio")

# PostgreSQL - Railway дает DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not set!")
if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY not set!")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL not set!")

# Парсим DATABASE_URL для asyncpg
def parse_database_url(url: str) -> dict:
    """Парсит DATABASE_URL в dict для asyncpg"""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": parsed.username,
        "password": parsed.password,
        "database": parsed.path[1:]  # убираем /
    }

DB_CONFIG = parse_database_url(DATABASE_URL)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Глобальные переменные
db_pool = None
business_connections = {}
chat_histories = defaultdict(list)  # История чатов в памяти
cached_system_prompt = None  # Кэш промпта
last_prompt_update = None  # Когда обновляли промпт

# Состояния
class ConfigStates(StatesGroup):
    waiting_for_json = State()


# ==================== БАЗА ДАННЫХ ====================
async def init_db():
    """Инициализация БД и создание таблиц"""
    global db_pool
    
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS style_messages (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_settings (
                key VARCHAR(50) PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS business_connections (
                connection_id VARCHAR(100) PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица для истории чатов (персистентная)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                role VARCHAR(20) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Индекс для быстрого поиска истории
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_chat_history_user_id 
            ON chat_history(user_id, created_at DESC)
        ''')
        
        await conn.execute('''
            INSERT INTO bot_settings (key, value) 
            VALUES ('enabled', 'false')
            ON CONFLICT (key) DO NOTHING
        ''')
    
    print("✅ Таблицы созданы/проверены")


async def get_setting(key: str) -> str:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT value FROM bot_settings WHERE key = $1", key
        )
        return result


async def set_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO bot_settings (key, value) 
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        ''', key, value)


async def get_all_messages() -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT message FROM style_messages ORDER BY id")
        return [row['message'] for row in rows]


async def add_messages(messages: list):
    async with db_pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO style_messages (message) VALUES ($1)",
            [(msg,) for msg in messages]
        )


async def get_messages_count() -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM style_messages")


async def clear_messages():
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM style_messages")


async def load_business_connections() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT connection_id, owner_id FROM business_connections")
        return {row['connection_id']: row['owner_id'] for row in rows}


async def save_business_connection(connection_id: str, owner_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO business_connections (connection_id, owner_id) 
            VALUES ($1, $2)
            ON CONFLICT (connection_id) DO UPDATE SET owner_id = $2
        ''', connection_id, owner_id)


async def delete_business_connection(connection_id: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM business_connections WHERE connection_id = $1",
            connection_id
        )


# ==================== ИСТОРИЯ ЧАТОВ ====================
async def save_to_history(user_id: int, role: str, content: str):
    """Сохраняет сообщение в историю (БД и память)"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO chat_history (user_id, role, content) VALUES ($1, $2, $3)",
            user_id, role, content
        )
    
    # Добавляем в память
    chat_histories[user_id].append({"role": role, "content": content})
    
    # Ограничиваем историю в памяти (последние 20 сообщений)
    if len(chat_histories[user_id]) > 20:
        chat_histories[user_id] = chat_histories[user_id][-20:]


async def load_chat_history(user_id: int, limit: int = 15) -> list:
    """Загружает последнюю историю чата из БД"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT role, content FROM chat_history 
            WHERE user_id = $1 
            ORDER BY created_at DESC 
            LIMIT $2
            """,
            user_id, limit
        )
        # Переворачиваем, чтобы было от старых к новым
        return [{"role": row['role'], "content": row['content']} for row in reversed(rows)]


async def clear_old_history():
    """Очищает историю старше 7 дней"""
    async with db_pool.acquire() as conn:
        cutoff_date = datetime.now() - timedelta(days=7)
        deleted = await conn.fetchval(
            "DELETE FROM chat_history WHERE created_at < $1 RETURNING COUNT(*)",
            cutoff_date
        )
        if deleted:
            print(f"🗑 Очищено {deleted} старых сообщений из истории")


# ==================== АНАЛИЗ ЧАТОВ ====================
def analyze_chat_export(json_data):
    messages = []
    try:
        if isinstance(json_data, dict):
            chat_messages = json_data.get("messages", [json_data])
        else:
            chat_messages = json_data
        
        for msg in chat_messages:
            if isinstance(msg, dict):
                text = ""
                if "text" in msg:
                    if isinstance(msg["text"], str):
                        text = msg["text"]
                    elif isinstance(msg["text"], list):
                        text = " ".join([
                            item["text"] if isinstance(item, dict) and "text" in item else str(item)
                            for item in msg["text"]
                        ])
                
                if text and len(text.strip()) > 0:
                    messages.append(text.strip())
        
        return messages
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        return []


def create_style_prompt(messages: list) -> str:
    """Создает улучшенный промпт для имитации стиля"""
    if not messages:
        return "Ты дружелюбный помощник."
    
    # Берем больше примеров для лучшего понимания стиля
    sample = messages[:100] if len(messages) > 100 else messages
    examples = "\n".join([f"- {msg}" for msg in sample[:30]])
    
    prompt = f"""Ты @{OWNER_USERNAME}. Общайся ТОЧНО как он, копируя его стиль на 100%.

ПРИМЕРЫ ЕГО СООБЩЕНИЙ:
{examples}

ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА СТИЛЯ:
1. Используй ЕГО сленг: "крч", "типо", "пон", "го", "норм", "блин", "чёт", "щас", "ну", "шо", "хз", "похуй"
2. Пиши БЕЗ ЗАГЛАВНЫХ букв если он так делает (кроме начала предложения иногда)
3. НЕ ставь точки в конце если он не ставит
4. Будь неформальным, расслабленным, иногда грубоватым
5. Используй мат там где он использует (не цензурь себя)
6. Пиши КОРОТКИЕ сообщения, но иногда можешь написать длиннее если надо объяснить
7. Используй его словечки и фразочки типа "sha", "ага", "пон"
8. НЕ будь вежливым роботом - будь собой как обычный чел
9. Можешь послать, пошутить грубо, материться - если в стиле
10. Отвечай ПО ТЕМЕ и НОРМАЛЬНО, не просто "sha" - веди диалог

ВАЖНО:
- Ты НЕ AI ассистент
- Ты просто общаешься как обычный человек
- Можешь быть резким, шутить, материться
- Пиши естественно, не старайся быть "полезным ботом"
- Если тебя спрашивают что-то - отвечай нормально, а не односложно
- Веди РЕАЛЬНЫЙ диалог, а не просто кидай "sha" "пон"

Общайся как @{OWNER_USERNAME}, копируй его манеру, его словечки, его подход."""
    
    return prompt


async def get_or_update_cached_prompt() -> str:
    """Возвращает кэшированный промпт или обновляет его"""
    global cached_system_prompt, last_prompt_update
    
    # Обновляем кэш раз в 5 минут или если его нет
    now = datetime.now()
    if (cached_system_prompt is None or 
        last_prompt_update is None or 
        (now - last_prompt_update).total_seconds() > 300):
        
        messages = await get_all_messages()
        cached_system_prompt = create_style_prompt(messages)
        last_prompt_update = now
        print("🔄 Промпт обновлен и закэширован")
    
    return cached_system_prompt


# ==================== ЗАПРОС К GROQ ====================
async def get_ai_response(message_text: str, user_id: int) -> str:
    """Генерирует ответ с учетом истории чата"""
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate"
    }
    
    # Получаем системный промпт из кэша
    system_prompt = await get_or_update_cached_prompt()
    
    # Загружаем историю если ее нет в памяти
    if user_id not in chat_histories or len(chat_histories[user_id]) == 0:
        chat_histories[user_id] = await load_chat_history(user_id, limit=15)
    
    # Формируем сообщения для API
    messages = [{"role": "system", "content": system_prompt}]
    
    # Добавляем историю (последние 15 сообщений)
    history = chat_histories[user_id][-15:]
    messages.extend(history)
    
    # Добавляем текущее сообщение
    messages.append({"role": "user", "content": message_text})
    
    data = {
        "model": "llama-3.3-70b-versatile",
        "messages": messages,
        "temperature": 0.9,  # Повышаем для более естественных ответов
        "max_tokens": 1000,  # Больше токенов для нормальных ответов
        "top_p": 0.95
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    ai_reply = result['choices'][0]['message']['content']
                    
                    # Сохраняем в историю
                    await save_to_history(user_id, "user", message_text)
                    await save_to_history(user_id, "assistant", ai_reply)
                    
                    return ai_reply
                else:
                    error_text = await response.text()
                    print(f"❌ Groq API ошибка {response.status}: {error_text}")
                    return "блин щас какая-то ошибка, попробуй еще раз"
    except asyncio.TimeoutError:
        print("⏰ Groq API таймаут")
        return "хз чёт долго грузит, попробуй еще раз"
    except Exception as e:
        print(f"❌ Ошибка Groq: {e}")
        return "крч какая-то ошибка вышла"


# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📤 Загрузить чаты")],
            [KeyboardButton(text="✅ Включить"), KeyboardButton(text="❌ Выключить")],
            [KeyboardButton(text="🗑 Очистить чаты"), KeyboardButton(text="🧹 Очистить историю")]
        ],
        resize_keyboard=True
    )
    return keyboard


# ==================== КОМАНДЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        await message.answer("Этот бот только для владельца")
        return
    
    await message.answer(
        f"👋 Привет, @{OWNER_USERNAME}!\n\n"
        f"🤖 Я буду отвечать за тебя в Telegram Business\n\n"
        f"Что умею:\n"
        f"📤 Загрузить чаты - импорт твоего стиля общения\n"
        f"✅ Включить - начать автоответы\n"
        f"❌ Выключить - остановить\n"
        f"📊 Статус - инфо о боте\n"
        f"🗑 Очистить чаты - удалить примеры\n"
        f"🧹 Очистить историю - удалить историю диалогов\n\n"
        f"Чтобы начать:\n"
        f"1. Экспортируй свои чаты (JSON)\n"
        f"2. Загрузи через 📤 Загрузить чаты\n"
        f"3. Включи автоответы через ✅ Включить",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "📊 Статус")
async def show_status(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return
    
    count = await get_messages_count()
    enabled = await get_setting("enabled")
    enabled_text = "✅ ВКЛЮЧЕНЫ" if enabled == "true" else "❌ ВЫКЛЮЧЕНЫ"
    connections = len(business_connections)
    
    # Считаем сообщения в истории
    async with db_pool.acquire() as conn:
        history_count = await conn.fetchval("SELECT COUNT(*) FROM chat_history")
        users_count = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM chat_history")
    
    conn_details = ""
    for conn_id, owner_id in business_connections.items():
        conn_details += f"\n  • ...{conn_id[-10:]} → {owner_id}"
    
    await message.answer(
        f"📊 Статус бота:\n\n"
        f"💾 Примеров стиля: {count}\n"
        f"Готовность: {'✅ Готов' if count >= 10 else '⚠️ Нужно минимум 10'}\n"
        f"Автоответы: {enabled_text}\n"
        f"Бизнес-подключений: {connections}{conn_details}\n\n"
        f"📝 История диалогов:\n"
        f"  • Всего сообщений: {history_count}\n"
        f"  • Пользователей: {users_count}\n"
        f"  • В памяти: {len(chat_histories)} чатов",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "📤 Загрузить чаты")
async def upload_chats(message: types.Message, state: FSMContext):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return

    await state.set_state(ConfigStates.waiting_for_json)
    await message.answer(
        "📤 Отправь JSON файлы с экспортом твоих чатов.\n\n"
        "Как экспортировать:\n"
        "1. Telegram Desktop → диалог\n"
        "2. Три точки → Export chat history\n"
        "3. Format: JSON\n"
        "4. Отправь файлы сюда\n\n"
        "Отправь /cancel чтобы отменить",
        reply_markup=ReplyKeyboardRemove()
    )


@dp.message(Command("cancel"))
async def cancel_upload(message: types.Message, state: FSMContext):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return
    
    await state.clear()
    await message.answer("❌ Отменено", reply_markup=get_main_keyboard())


@dp.message(ConfigStates.waiting_for_json, F.document)
async def process_json(message: types.Message, state: FSMContext):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return

    document = message.document
    
    if not document.file_name.endswith('.json'):
        await message.answer("⚠️ Нужен JSON файл!")
        return
    
    await message.answer("⏳ Обрабатываю...")
    
    try:
        file = await bot.get_file(document.file_id)
        file_path = f"/tmp/temp_{document.file_name}"
        await bot.download_file(file.file_path, file_path)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        messages = analyze_chat_export(json_data)
        
        if messages:
            await add_messages(messages)
            total = await get_messages_count()
            
            # Сбрасываем кэш промпта чтобы обновился
            global cached_system_prompt
            cached_system_prompt = None
            
            await message.answer(
                f"✅ Загружено {len(messages)} сообщений!\n"
                f"💾 Всего в БД: {total}\n\n"
                f"{'✅ Можешь включать автоответы!' if total >= 10 else '⚠️ Нужно еще примеров'}",
                reply_markup=get_main_keyboard()
            )
        else:
            await message.answer("⚠️ Не нашел сообщений в файле", reply_markup=get_main_keyboard())
        
        os.remove(file_path)
        await state.clear()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=get_main_keyboard())
        await state.clear()


@dp.message(F.text == "🗑 Очистить чаты")
async def clear_chats(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return
    
    await clear_messages()
    await set_setting("enabled", "false")
    
    # Сбрасываем кэш
    global cached_system_prompt
    cached_system_prompt = None
    
    await message.answer(
        "🗑 Все примеры удалены!\n"
        "Автоответы выключены.",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "🧹 Очистить историю")
async def clear_history(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return
    
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("DELETE FROM chat_history RETURNING COUNT(*)")
    
    # Очищаем память
    chat_histories.clear()
    
    await message.answer(
        f"🧹 История очищена!\n"
        f"Удалено {count} сообщений.",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "✅ Включить")
async def enable_bot(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return

    count = await get_messages_count()
    if count < 10:
        await message.answer(
            f"⚠️ Сначала загрузи чаты!\n"
            f"Сейчас: {count} сообщений\n"
            f"Нужно минимум: 10",
            reply_markup=get_main_keyboard()
        )
        return
    
    await set_setting("enabled", "true")
    await message.answer(
        "✅ Автоответы ВКЛЮЧЕНЫ!\n\n"
        "Теперь когда тебе пишут в Telegram Business - я отвечаю за тебя 😎\n"
        "Буду вести нормальные диалоги с историей!",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "❌ Выключить")
async def disable_bot(message: types.Message):
    if message.from_user.username and message.from_user.username.lower() != OWNER_USERNAME.lower():
        return

    await set_setting("enabled", "false")
    await message.answer(
        "❌ Автоответы ВЫКЛЮЧЕНЫ",
        reply_markup=get_main_keyboard()
    )


# ==================== BUSINESS HANDLERS ====================
@dp.business_connection()
async def handle_business_connection(business_connection: types.BusinessConnection):
    global business_connections
    
    try:
        owner_id = business_connection.user.id
        connection_id = business_connection.id
        is_enabled = business_connection.is_enabled

        if is_enabled:
            business_connections[connection_id] = owner_id
            await save_business_connection(connection_id, owner_id)
            print(f"✅ Бизнес-подключение: {connection_id}")
            print(f"   Владелец: @{business_connection.user.username} (ID: {owner_id})")
        else:
            if connection_id in business_connections:
                del business_connections[connection_id]
            await delete_business_connection(connection_id)
            print(f"❌ Отключено: {connection_id}")

        print(f"📊 Всего подключений: {len(business_connections)}")

    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")


@dp.business_message(F.text)
async def handle_business_message(message: types.Message):
    try:
        connection_id = message.business_connection_id

        if not connection_id:
            return

        if connection_id not in business_connections:
            print(f"⚠️ Неизвестное подключение: {connection_id}")
            return

        owner_id = business_connections[connection_id]
        sender_id = message.from_user.id

        print(f"📨 Сообщение в бизнес-чате:")
        print(f"   От: @{message.from_user.username} (ID: {sender_id})")
        print(f"   Текст: {message.text[:100]}...")

        if sender_id == owner_id:
            # Сохраняем сообщение владельца в историю чтобы бот знал контекст
            # Но не отвечаем на него
            print(f"💬 Сообщение от владельца - сохраняю в контекст")
            # Можно сохранить как assistant чтобы бот знал что владелец уже ответил
            await save_to_history(sender_id, "assistant", message.text)
            return

        enabled = await get_setting("enabled")
        if enabled != "true":
            print(f"⏭️ Бот выключен - пропускаем")
            return

        count = await get_messages_count()
        if count < 10:
            print(f"⏭️ Недостаточно примеров ({count}) - пропускаем")
            return

        print(f"🤖 Генерирую ответ с историей...")

        client_chat_id = sender_id

        try:
            await bot.send_chat_action(
                chat_id=client_chat_id,
                action="typing",
                business_connection_id=connection_id
            )
        except Exception as e:
            print(f"⚠️ send_chat_action ошибка: {e}")

        # Генерируем ответ с учетом истории
        ai_response = await get_ai_response(message.text, sender_id)

        await bot.send_message(
            chat_id=client_chat_id,
            text=ai_response,
            business_connection_id=connection_id
        )

        print(f"✅ Ответ: {ai_response[:100]}...")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


# ==================== ЗАПУСК ====================
async def main():
    global business_connections
    
    await init_db()
    
    business_connections = await load_business_connections()
    
    # Очищаем старую историю при запуске
    await clear_old_history()
    
    count = await get_messages_count()
    enabled = await get_setting("enabled")
    
    # Прогреваем кэш промпта
    if count > 0:
        await get_or_update_cached_prompt()
    
    print("🚀 Бот запущен!")
    print(f"👤 Владелец: @{OWNER_USERNAME}")
    print(f"💾 Сообщений в БД: {count}")
    print(f"🔄 Автоответы: {'ВКЛ' if enabled == 'true' else 'ВЫКЛ'}")
    print(f"📊 Бизнес-подключений: {len(business_connections)}")
    print(f"📝 История: включена с кэшированием")
    
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())