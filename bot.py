import logging
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from connector import initialize_pool, get_connection, release_connection, close_pool

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize database connection pool
try:
    initialize_pool()
    logger.info("Database connection pool initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize database connection pool: {e}")
    exit(1)

# Fetch drama details from the database
def fetch_drama_details(drama_name):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Query drama details
        cursor.execute("""
            SELECT title, year, rating, rating_count, description, country,
                   episodes, airing_start, airing_end, network, duration,
                   content_rating, genre, trailer_url, image_url
            FROM dramas
            WHERE title ILIKE %s
            LIMIT 1
        """, (f"%{drama_name}%",))
        result = cursor.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error fetching drama details: {e}")
        return None
    finally:
        if conn:
            release_connection(conn)

# Format drama details for the bot response
def format_drama_details(drama, user_name):
    if not drama:
        return f"Sorry, {user_name}, no details found for the requested drama."

    title, year, rating, rating_count, description, country, episodes, airing_start, airing_end, network, duration, content_rating, genre, trailer_url, image_url = drama

    formatted_start = airing_start.strftime("%b %d, %Y") if airing_start else "Unknown"
    formatted_end = airing_end.strftime("%b %d, %Y") if airing_end else "Unknown"
    formatted_airing = f"{formatted_start} - {formatted_end}"

    formatted_genre = " ".join([f"#{g.strip()}" for g in genre.split(",")]) if genre else "Unknown"

    return f"""
**{user_name} via @MoviesGamabot**
**{title} ({year})**
⭐️ Rating: {rating}/10 from {rating_count} users [MyDramaList]({image_url})

`{description}`

**Country**: `{country}`
**Episodes**: `{episodes}`
**Aired**: `{formatted_airing}`
**Network**: `{network}`
**Duration**: `{duration}`
**Content Rating**: `{content_rating}`

**Genre**: {formatted_genre}

[▶️ Watch Trailer]({trailer_url})
    """

# Bot handlers
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Bot started by user: {update.effective_user.first_name}")
    await update.message.reply_text("Hello! Send me a drama name to search.")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    user_name = update.effective_user.first_name
    logger.info(f"User '{user_name}' searched for: {user_input}")

    # Fetch and format drama details
    drama_details = fetch_drama_details(user_input)
    response = format_drama_details(drama_details, user_name)

    # Reply with the details
    await update.message.reply_text(response, parse_mode="Markdown")

# Main function
if __name__ == "__main__":
    # Create the application
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # Ensure database pool is closed on exit
    import atexit
    atexit.register(close_pool)

    logger.info("Bot is running...")
    application.run_polling()
