# Add at the beginning of your bot.py or create health.py
from fastapi import FastAPI, Response
import uvicorn

# Create FastAPI app for health checks
app = FastAPI(title="Telegram Bot Health")

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway."""
    return {"status": "healthy", "service": "telegram-bot"}

@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Telegram Bot is running"}

# Run this in a separate thread in your main() function
import threading

def run_health_server():
    """Run the health check server in a separate thread."""
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

# In your main() function:
def main():
    """Initialize and run the bot."""
    # Start health check server in a separate thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Rest of your bot initialization code...
    # ... your existing main() code ...