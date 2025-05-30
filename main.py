#!/usr/bin/env python3
"""
Emerald's Killfeed - Discord Bot for Deadside PvP Engine
Full production-grade bot with killfeed parsing, stats, economy, and premium features
"""

import asyncio
import logging
import os
import sys
import json
import hashlib
import re
import time
from pathlib import Path

# Clean up any conflicting discord modules before importing
for module_name in list(sys.modules.keys()):
    if module_name == 'discord' or module_name.startswith('discord.'):
        del sys.modules[module_name]

# Import py-cord v2.6.1
try:
    import discord
    from discord.ext import commands
    print(f"✅ Successfully imported py-cord")
except ImportError as e:
    print(f"❌ Error importing py-cord: {e}")
    print("Please ensure py-cord 2.6.1 is installed")
    sys.exit(1)

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.models.database import DatabaseManager
from bot.parsers.killfeed_parser import KillfeedParser
from bot.parsers.historical_parser import HistoricalParser
from bot.parsers.unified_log_parser import UnifiedLogParser

# Load environment variables (optional for Railway)
load_dotenv()

# Detect Railway environment
RAILWAY_ENV = os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_STATIC_URL")
if RAILWAY_ENV:
    print(f"🚂 Running on Railway environment")
else:
    print("🖥️ Running in local/development environment")

# Import Railway keep-alive server
from keep_alive import keep_alive

# Set runtime mode to production
MODE = os.getenv("MODE", "production")
print(f"Runtime mode set to: {MODE}")

# Start keep-alive server for Railway deployment
if MODE == "production" or RAILWAY_ENV:
    print("🚀 Starting Railway keep-alive server...")
    keep_alive()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EmeraldKillfeedBot(commands.Bot):
    """Main bot class for Emerald's Killfeed"""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            status=discord.Status.online,
            activity=discord.Game(name="Emerald's Killfeed v2.0")
        )

        # Initialize variables
        self.db_manager = None
        self.scheduler = AsyncIOScheduler()
        self.killfeed_parser = None
        self.log_parser = None
        self.historical_parser = None
        self.unified_log_parser = None
        self.ssh_connections = []

        # Missing essential properties
        self.assets_path = Path('./assets')
        self.dev_data_path = Path('./dev_data')
        self.dev_mode = os.getenv('DEV_MODE', 'false').lower() == 'true'

        logger.info("Bot initialized in production mode")

    async def load_cogs(self):
        """Load all bot cogs using proper py-cord methods"""
        try:
            # List of cogs to load
            cogs = [
                'bot.cogs.core',
                'bot.cogs.admin_channels', 
                'bot.cogs.admin_batch',
                'bot.cogs.linking',
                'bot.cogs.stats',
                'bot.cogs.leaderboards_fixed',
                'bot.cogs.automated_leaderboard',
                'bot.cogs.economy',
                'bot.cogs.gambling',
                'bot.cogs.bounties',
                'bot.cogs.factions',
                'bot.cogs.premium',
                'bot.cogs.parsers'
            ]

            loaded_cogs = []
            failed_cogs = []

            for cog in cogs:
                try:
                    # Use synchronous load_extension (not awaitable in this py-cord version)
                    self.load_extension(cog)
                    loaded_cogs.append(cog)
                    logger.info(f"✅ Successfully loaded cog: {cog}")
                except Exception as e:
                    failed_cogs.append(cog)
                    logger.error(f"❌ Failed to load cog {cog}: {e}")
                    import traceback
                    logger.error(f"Cog error traceback: {traceback.format_exc()}")

            # Give py-cord time to process the cogs and register commands
            await asyncio.sleep(2)

            # Check command registration with comprehensive debugging
            command_count = 0
            command_source = "none"
            command_names = []

            # Check all possible command storage locations
            if hasattr(self, 'pending_application_commands') and self.pending_application_commands:
                command_count = len(self.pending_application_commands)
                command_names = [cmd.name for cmd in self.pending_application_commands]
                command_source = "pending_application_commands"
            elif hasattr(self, 'application_commands') and self.application_commands:
                command_count = len(self.application_commands)
                command_names = [cmd.name for cmd in self.application_commands]
                command_source = "application_commands"

            logger.info(f"📊 Loaded {len(loaded_cogs)}/{len(cogs)} cogs successfully")
            logger.info(f"📊 Total slash commands registered: {command_count} (via {command_source})")

            if command_count > 0:
                logger.info(f"🔍 Commands found: {', '.join(command_names[:10])}{'...' if len(command_names) > 10 else ''}")
            else:
                logger.error("❌ NO COMMANDS REGISTERED - Cog loading failed to register commands")
                # Debug all cogs to see what they contain
                for cog_name in loaded_cogs:
                    try:
                        cog_obj = self.get_cog(cog_name.split('.')[-1].title().replace('_', ''))
                        if cog_obj:
                            cog_commands = [cmd for cmd in dir(cog_obj) if hasattr(getattr(cog_obj, cmd), '__annotations__')]
                            logger.debug(f"Cog {cog_name} methods: {cog_commands}")
                    except:
                        pass

            if failed_cogs:
                logger.error(f"❌ Failed cogs: {failed_cogs}")
                return False

            if command_count == 0:
                logger.error("❌ Critical: No commands registered despite successful cog loading")
                return False

            logger.info("✅ All cogs loaded and commands registered successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Critical failure loading cogs: {e}")
            import traceback
            logger.error(f"Load cogs traceback: {traceback.format_exc()}")
            return False

    async def register_commands_safely(self):
        """
        ADVANCED Command Sync Logic - Global with Guild Fallback
        Implements requirements: global sync first, then per-guild fallback on failure
        """
        try:
            # Try multiple attributes to find commands
            command_count = 0
            commands_source = "none"
            command_names = []

            if hasattr(self, 'application_commands') and self.application_commands:
                command_count = len(self.application_commands)
                command_names = [cmd.name for cmd in self.application_commands]
                commands_source = "application_commands"
            elif hasattr(self, 'pending_application_commands') and self.pending_application_commands:
                command_count = len(self.pending_application_commands)
                command_names = [cmd.name for cmd in self.pending_application_commands]
                commands_source = "pending_application_commands"
            elif hasattr(self, 'slash_commands') and self.slash_commands:
                command_count = len(self.slash_commands)
                command_names = [cmd.name for cmd in self.slash_commands]
                commands_source = "slash_commands"

            logger.info(f"📊 {command_count} commands found via {commands_source}")

            # Debug: Show actual command names
            if command_count > 0:
                logger.info(f"🔍 Commands to sync: {', '.join(command_names[:10])}{'...' if len(command_names) > 10 else ''}")
            else:
                # Debug all available attributes
                attrs = [attr for attr in dir(self) if 'command' in attr.lower()]
                logger.warning(f"🔍 Available command attributes: {attrs}")
                logger.warning("⚠️ No commands to sync - this may indicate a cog loading issue")
                return

            # Check for existing rate limit
            rate_limit_file = "rate_limit_cooldown.txt"
            if os.path.exists(rate_limit_file):
                try:
                    with open(rate_limit_file, 'r') as f:
                        cooldown_until = float(f.read().strip())
                        if time.time() < cooldown_until:
                            remaining = int(cooldown_until - time.time())
                            logger.warning(f"⏳ Rate limit active for {remaining}s - skipping sync")
                            return
                        else:
                            os.remove(rate_limit_file)
                            logger.info("✅ Rate limit cooldown expired")
                except:
                    pass

            # STEP 1: Try global sync first (per requirements)
            logger.info(f"🌍 Attempting GLOBAL command sync...")
            try:
                await asyncio.wait_for(self.sync_commands(), timeout=30)
                logger.info(f"✅ GLOBAL SYNC SUCCESSFUL - All guilds updated instantly")

                # Save success marker
                with open("global_sync_success.txt", 'w') as f:
                    f.write(str(time.time()))
                return

            except asyncio.TimeoutError:
                logger.warning("⏰ Global sync timed out - proceeding to guild fallback")
            except Exception as e:
                error_msg = str(e).lower()
                if "rate limited" in error_msg or "429" in error_msg:
                    logger.error(f"❌ Global sync rate limited: {e}")

                    # Extract retry time and save cooldown for future attempts
                    retry_match = re.search(r'Retrying in ([\d.]+) seconds', str(e))
                    if retry_match:
                        retry_time = float(retry_match.group(1))
                        cooldown_until = time.time() + retry_time + 60
                        with open(rate_limit_file, 'w') as f:
                            f.write(str(cooldown_until))
                        logger.error(f"💾 Rate limit cooldown saved for {retry_time + 60}s")

                    # Continue to guild fallback instead of returning
                    logger.info("🏠 Global sync rate limited - proceeding to guild fallback")
                else:
                    logger.warning(f"⚠️ Global sync failed: {e} - proceeding to guild fallback")

            # STEP 2: Guild-specific fallback (per requirements)
            logger.info(f"🏠 Attempting PER-GUILD sync fallback for {len(self.guilds)} guilds...")
            success_count = 0
            rate_limited = False

            for guild in self.guilds:
                if rate_limited:
                    break

                try:
                    # Use py-cord specific guild sync method with guild_ids parameter
                    await asyncio.wait_for(self.sync_commands(guild_ids=[guild.id]), timeout=15)
                    success_count += 1
                    logger.info(f"✅ GUILD SYNC SUCCESSFUL: {guild.name}")

                    # Small delay between guild syncs to avoid rate limits
                    if success_count < len(self.guilds):
                        await asyncio.sleep(3)

                except Exception as guild_error:
                    error_msg = str(guild_error).lower()
                    if "rate limited" in error_msg or "429" in error_msg:
                        logger.error(f"❌ Guild sync rate limited for {guild.name}")
                        rate_limited = True
                        break
                    else:
                        logger.warning(f"⚠️ Guild sync failed for {guild.name}: {guild_error}")

            if success_count > 0:
                logger.info(f"✅ GUILD FALLBACK COMPLETED: {success_count}/{len(self.guilds)} successful")
                return
            else:
                logger.warning("⚠️ ALL SYNC METHODS FAILED - Commands will sync on next restart")

        except Exception as e:
            logger.error(f"❌ Command sync system failed: {e}")
            import traceback
            logger.error(f"Sync traceback: {traceback.format_exc()}")

    async def cleanup_connections(self):
        """Clean up AsyncSSH connections on shutdown"""
        try:
            if hasattr(self, 'killfeed_parser') and self.killfeed_parser:
                await self.killfeed_parser.cleanup_sftp_connections()

            if hasattr(self, 'unified_log_parser') and self.unified_log_parser:
                # Clean up unified parser SFTP connections
                for pool_key, conn in list(self.unified_log_parser.sftp_connections.items()):
                    try:
                        if not conn.is_closed():
                            conn.close()
                    except:
                        pass
                self.unified_log_parser.sftp_connections.clear()

            logger.info("Cleaned up all SFTP connections")

        except Exception as e:
            logger.error(f"Failed to cleanup connections: {e}")

    async def setup_database(self):
        """Setup MongoDB connection"""
        mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MongoDB URI not found in environment variables")
            return False

        try:
            self.mongo_client = AsyncIOMotorClient(mongo_uri)
            self.database = self.mongo_client.emerald_killfeed

            # Initialize database manager with PHASE 1 architecture
            from bot.models.database import DatabaseManager
            self.db_manager = DatabaseManager(self.mongo_client)
            # For backward compatibility
            self.database = self.db_manager

            # Test connection
            await self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")

            # Initialize database indexes
            await self.db_manager.initialize_indexes()
            logger.info("Database architecture initialized (PHASE 1)")

            # Initialize batch sender for rate limit management
            from bot.utils.batch_sender import BatchSender
            self.batch_sender = BatchSender(self)

            # Initialize advanced rate limiter
            from bot.utils.advanced_rate_limiter import AdvancedRateLimiter
            self.advanced_rate_limiter = AdvancedRateLimiter(self)

            # Initialize parsers (PHASE 2) - Data parsers for killfeed & log events
            self.killfeed_parser = KillfeedParser(self)
            self.historical_parser = HistoricalParser(self)
            self.unified_log_parser = UnifiedLogParser(self)
            logger.info("Parsers initialized (PHASE 2) + Unified Log Parser + Advanced Rate Limiter + Batch Sender")

            return True

        except Exception as e:
            logger.error("Failed to connect to MongoDB: %s", e)
            return False

    def setup_scheduler(self):
        """Setup background job scheduler"""
        try:
            self.scheduler.start()
            logger.info("Background job scheduler started")
            return True
        except Exception as e:
            logger.error("Failed to start scheduler: %s", e)
            return False

    async def on_ready(self):
        """Called when bot is ready and connected to Discord"""
        # Only run setup once
        if hasattr(self, '_setup_complete'):
            return

        logger.info("🚀 Bot is ready! Starting bulletproof setup...")

        try:
            # STEP 1: Load cogs with proper async loading
            logger.info("🔧 Loading cogs for command registration...")
            cogs_success = await self.load_cogs()

            if not cogs_success:
                logger.error("❌ Cog loading failed - aborting setup")
                return

            logger.info("✅ Cog loading: Complete")

            # STEP 2: Verify commands are actually registered
            command_count = 0
            if hasattr(self, 'pending_application_commands'):
                command_count = len(self.pending_application_commands)
            elif hasattr(self, 'application_commands'):
                command_count = len(self.application_commands)

            if command_count == 0:
                logger.error("❌ CRITICAL: No commands found after cog loading - fix required")
                return

            logger.info(f"✅ {command_count} commands registered and ready for sync")

            # STEP 3: Command sync - simplified and robust
            logger.info("🔧 Starting command sync...")
            try:
                await self.register_commands_safely()
                logger.info("✅ Command sync completed")
            except Exception as sync_error:
                logger.error(f"❌ Command sync failed: {sync_error}")

            # STEP 4: Database setup
            logger.info("🚀 Starting database and parser setup...")
            db_success = await self.setup_database()
            if not db_success:
                logger.error("❌ Database setup failed")
                return
            logger.info("✅ Database setup: Success")

            # STEP 5: Scheduler setup
            scheduler_success = self.setup_scheduler()
            if not scheduler_success:
                logger.error("❌ Scheduler setup failed")
                return
            logger.info("✅ Scheduler setup: Success")

            # STEP 6: Schedule parsers
            if self.killfeed_parser:
                self.killfeed_parser.schedule_killfeed_parser()
                logger.info("📡 Killfeed parser scheduled")

            if self.unified_log_parser:
                try:
                    # Remove existing job if it exists
                    try:
                        self.scheduler.remove_job('unified_log_parser')
                    except:
                        pass

                    self.scheduler.add_job(
                        self.unified_log_parser.run_log_parser,
                        'interval',
                        seconds=180,
                        id='unified_log_parser',
                        max_instances=1,
                        coalesce=True
                    )
                    logger.info("📜 Unified log parser scheduled (180s interval)")

                    # Run initial parse
                    asyncio.create_task(self.unified_log_parser.run_log_parser())
                    logger.info("🔥 Initial unified log parser run triggered")

                except Exception as e:
                    logger.error(f"Failed to schedule unified log parser: {e}")

            # STEP 7: Final status
            if self.user:
                logger.info("✅ Bot logged in as %s (ID: %s)", self.user.name, self.user.id)
            logger.info("✅ Connected to %d guilds", len(self.guilds))

            for guild in self.guilds:
                logger.info(f"📡 Bot connected to: {guild.name} (ID: {guild.id})")

            # Verify assets exist
            if self.assets_path.exists():
                assets = list(self.assets_path.glob('*.png'))
                logger.info("📁 Found %d asset files", len(assets))
            else:
                logger.warning("⚠️ Assets directory not found")

            logger.info("🎉 Bot setup completed successfully!")
            self._setup_complete = True

        except Exception as e:
            logger.error(f"❌ Critical error in bot setup: {e}")
            import traceback
            logger.error(f"Setup error traceback: {traceback.format_exc()}")
            raise

    async def on_guild_join(self, guild):
        """Called when bot joins a new guild - NO SYNC to prevent rate limits"""
        logger.info("Joined guild: %s (ID: %s)", guild.name, guild.id)
        logger.info("Commands will be available after next restart (bulletproof mode)")

    async def on_guild_remove(self, guild):
        """Called when bot is removed from a guild"""
        logger.info("Left guild: %s (ID: %s)", guild.name, guild.id)

    async def close(self):
        """Clean shutdown"""
        logger.info("Shutting down bot...")

        # Clean up SFTP connections
        await self.cleanup_connections()

        # Flush advanced rate limiter if it exists
        if hasattr(self, 'advanced_rate_limiter'):
            await self.advanced_rate_limiter.flush_all_queues()
            logger.info("Advanced rate limiter flushed")

        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

        await super().close()
        logger.info("Bot shutdown complete")

    async def shutdown(self):
        """Graceful shutdown"""
        try:
            # Flush any remaining batched messages
            if hasattr(self, 'batch_sender'):
                logger.info("Flushing remaining batched messages...")
                await self.batch_sender.flush_all_queues()
                logger.info("Batch sender flushed")

            # Flush advanced rate limiter
            if hasattr(self, 'advanced_rate_limiter'):
                logger.info("Flushing advanced rate limiter...")
                await self.advanced_rate_limiter.flush_all_queues()
                logger.info("Advanced rate limiter flushed")

            # Clean up SFTP connections
            await self.cleanup_connections()

            if self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("Scheduler stopped")

            if hasattr(self, 'mongo_client') and self.mongo_client:
                self.mongo_client.close()
                logger.info("MongoDB connection closed")

            await super().close()
            logger.info("Bot shutdown complete")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

async def main():
    """Main entry point"""
    # Check required environment variables for Railway deployment
    bot_token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN')
    mongo_uri = os.getenv('MONGO_URI') or os.getenv('MONGODB_URI')
    tip4serv_key = os.getenv('TIP4SERV_KEY')  # Optional service key

    # Railway environment detection
    railway_env = os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('RAILWAY_STATIC_URL')
    if railway_env:
        print(f"✅ Railway environment detected")

    # Validate required secrets
    if not bot_token:
        logger.error("❌ BOT_TOKEN not found in environment variables")
        logger.error("Please set BOT_TOKEN in your Railway environment variables")
        return

    if not mongo_uri:
        logger.error("❌ MONGO_URI not found in environment variables") 
        logger.error("Please set MONGO_URI in your Railway environment variables")
        return

    # Log startup success
    logger.info(f"✅ Bot starting with token: {'*' * 20}...{bot_token[-4:] if bot_token else 'MISSING'}")
    logger.info(f"✅ MongoDB URI configured: {'*' * 20}...{mongo_uri[-10:] if mongo_uri else 'MISSING'}")
    if tip4serv_key:
        logger.info(f"✅ TIP4SERV_KEY configured: {'*' * 10}...{tip4serv_key[-4:]}")
    else:
        logger.info("ℹ️ TIP4SERV_KEY not configured (optional)")

    # Create and run bot
    print("Creating bot instance...")
    bot = EmeraldKillfeedBot()

    try:
        await bot.start(bot_token)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error("Error in bot execution: %s", e)
        raise
    finally:
        if not bot.is_closed():
            await bot.close()

if __name__ == "__main__":
    # Run the bot
    print("Starting main bot execution...")
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error in main execution: {e}")
        import traceback
        traceback.print_exc()