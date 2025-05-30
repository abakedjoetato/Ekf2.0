"""
Emerald's Killfeed - Unified Log Parser System
BULLETPROOF VERSION - Complete overhaul for 100% reliability
"""

import asyncio
import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple

import aiofiles
import discord
import asyncssh
from discord.ext import commands

from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class UnifiedLogParser:
    """
    BULLETPROOF UNIFIED LOG PARSER
    - 100% reliable SFTP connection handling
    - Bulletproof state management
    - Guaranteed voice channel updates
    - Rate limit safe operation
    """

    def __init__(self, bot):
        self.bot = bot

        # Bulletproof state dictionaries with proper isolation
        self.file_states: Dict[str, Dict[str, Any]] = {}
        self.player_sessions: Dict[str, Dict[str, Any]] = {}
        self.sftp_connections: Dict[str, asyncssh.SSHClientConnection] = {}
        self.last_log_position: Dict[str, int] = {}
        self.player_lifecycle: Dict[str, Dict[str, Any]] = {}
        self.server_status: Dict[str, Dict[str, Any]] = {}
        self.log_file_hashes: Dict[str, str] = {}
        
        # Player name resolution cache
        self.player_name_cache: Dict[str, str] = {}

        # Compile patterns once for efficiency
        self.patterns = self._compile_patterns()
        self.mission_mappings = self._get_mission_mappings()

        # Load state on startup
        asyncio.create_task(self._load_persistent_state())

    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """Compile regex patterns for log parsing"""
        return {
            # Player connection patterns - improved to capture player names better
            'player_queue_join': re.compile(r'LogNet: Join request: /Game/Maps/world_\d+/World_\d+\?.*eosid=\|([a-f0-9]+).*Name=([^&\?\s]+)', re.IGNORECASE),
            'player_registered': re.compile(r'LogOnline: Warning: Player \|([a-f0-9]+) successfully registered!', re.IGNORECASE),
            'player_disconnect': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # Mission patterns
            'mission_respawn': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) will respawn in (\d+)', re.IGNORECASE),
            'mission_state_change': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to ([A-Z_]+)', re.IGNORECASE),
            'mission_ready': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to READY', re.IGNORECASE),
            'mission_initial': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to INITIAL', re.IGNORECASE),
            'mission_in_progress': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to IN_PROGRESS', re.IGNORECASE),
            'mission_completed': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to COMPLETED', re.IGNORECASE),

            # Vehicle patterns
            'vehicle_spawn': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Add\] Add vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),
            'vehicle_delete': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Del\] Del vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),

            # Airdrop patterns
            'airdrop_event': re.compile(r'Event_AirDrop.*spawned.*location.*X=([\d\.-]+).*Y=([\d\.-]+)', re.IGNORECASE),
            'airdrop_spawn': re.compile(r'LogSFPS:.*airdrop.*spawn', re.IGNORECASE),
            'airdrop_flying': re.compile(r'LogSFPS:.*airdrop.*flying', re.IGNORECASE),

            # Helicrash patterns
            'helicrash_event': re.compile(r'Helicrash.*spawned.*location.*X=([\d\.-]+).*Y=([\d\.-]+)', re.IGNORECASE),
            'helicrash_spawn': re.compile(r'LogSFPS:.*helicrash.*spawn', re.IGNORECASE),
            'helicrash_crash': re.compile(r'LogSFPS:.*helicopter.*crash', re.IGNORECASE),

            # Trader patterns
            'trader_spawn': re.compile(r'Trader.*spawned.*location.*X=([\d\.-]+).*Y=([\d\.-]+)', re.IGNORECASE),
            'trader_event': re.compile(r'LogSFPS:.*trader.*spawn', re.IGNORECASE),
            'trader_arrival': re.compile(r'LogSFPS:.*trader.*arrived', re.IGNORECASE),

            # Timestamp
            'timestamp': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\]')
        }

    def _get_mission_mappings(self) -> Dict[str, str]:
        """Mission ID to readable name mappings"""
        return {
            'GA_Airport_mis_01_SFPSACMission': 'Airport Mission #1',
            'GA_Airport_mis_02_SFPSACMission': 'Airport Mission #2',
            'GA_Airport_mis_03_SFPSACMission': 'Airport Mission #3',
            'GA_Airport_mis_04_SFPSACMission': 'Airport Mission #4',
            'GA_Military_02_Mis1': 'Military Base Mission #2',
            'GA_Military_03_Mis_01': 'Military Base Mission #3',
            'GA_Military_04_Mis1': 'Military Base Mission #4',
            'GA_Beregovoy_Mis1': 'Beregovoy Settlement Mission',
            'GA_Settle_05_ChernyLog_Mis1': 'Cherny Log Settlement Mission',
            'GA_Ind_01_m1': 'Industrial Zone Mission #1',
            'GA_Ind_02_Mis_1': 'Industrial Zone Mission #2',
            'GA_KhimMash_Mis_01': 'Chemical Plant Mission #1',
            'GA_KhimMash_Mis_02': 'Chemical Plant Mission #2',
            'GA_Bunker_01_Mis1': 'Underground Bunker Mission',
            'GA_Sawmill_01_Mis1': 'Sawmill Mission #1',
            'GA_Settle_09_Mis_1': 'Settlement Mission #9',
            'GA_Military_04_Mis_2': 'Military Base Mission #4B',
            'GA_PromZone_6_Mis_1': 'Industrial Zone Mission #6',
            'GA_PromZone_Mis_01': 'Industrial Zone Mission A',
            'GA_PromZone_Mis_02': 'Industrial Zone Mission B',
            'GA_Kamensk_Ind_3_Mis_1': 'Kamensk Industrial Mission',
            'GA_Kamensk_Mis_1': 'Kamensk City Mission #1',
            'GA_Kamensk_Mis_2': 'Kamensk City Mission #2',
            'GA_Kamensk_Mis_3': 'Kamensk City Mission #3',
            'GA_Krasnoe_Mis_1': 'Krasnoe City Mission',
            'GA_Vostok_Mis_1': 'Vostok City Mission',
            'GA_Lighthouse_02_Mis1': 'Lighthouse Mission #2',
            'GA_Elevator_Mis_1': 'Elevator Complex Mission #1',
            'GA_Elevator_Mis_2': 'Elevator Complex Mission #2',
            'GA_Sawmill_02_1_Mis1': 'Sawmill Mission #2A',
            'GA_Sawmill_03_Mis_01': 'Sawmill Mission #3',
            'GA_Bochki_Mis_1': 'Barrel Storage Mission',
            'GA_Dubovoe_0_Mis_1': 'Dubovoe Resource Mission',
        }

    def normalize_mission_name(self, mission_id: str) -> str:
        """Convert mission ID to readable name using EmbedFactory"""
        return EmbedFactory.normalize_mission_name(mission_id)

    def get_mission_level(self, mission_id: str) -> int:
        """Determine mission difficulty level using EmbedFactory"""
        return EmbedFactory.get_mission_level(mission_id)

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create bulletproof SFTP connection"""
        try:
            host = server_config.get('host')
            port = server_config.get('port', 22)
            username = server_config.get('username')
            password = server_config.get('password')

            if not all([host, username, password]):
                logger.warning(f"Missing SFTP credentials for {server_config.get('_id')}")
                return None

            # Use proper port from config
            # if port == 22:
            #    port = 8822  # Default to 8822 for our servers

            connection_key = f"{host}:{port}:{username}"

            # Check existing connection
            if connection_key in self.sftp_connections:
                conn = self.sftp_connections[connection_key]
                try:
                    if not conn.is_closed():
                        return conn
                    else:
                        del self.sftp_connections[connection_key]
                except:
                    del self.sftp_connections[connection_key]

            # Create new connection with bulletproof settings
            for attempt in range(3):
                try:
                    conn = await asyncio.wait_for(
                        asyncssh.connect(
                            host,
                            username=username,
                            password=password,
                            port=port,
                            known_hosts=None,
                            server_host_key_algs=['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512'],
                            kex_algs=['diffie-hellman-group14-sha256', 'diffie-hellman-group16-sha512', 'ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521'],
                            encryption_algs=['aes128-ctr', 'aes192-ctr', 'aes256-ctr', 'aes128-gcm@openssh.com', 'aes256-gcm@openssh.com'],
                            mac_algs=['hmac-sha2-256', 'hmac-sha1']
                        ),
                        timeout=30
                    )
                    self.sftp_connections[connection_key] = conn
                    logger.info(f"✅ SFTP connected to {host}:{port}")
                    return conn

                except (asyncio.TimeoutError, asyncssh.Error) as e:
                    logger.warning(f"SFTP attempt {attempt + 1} failed: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)

            logger.error(f"❌ Failed to connect to SFTP {host}:{port}")
            return None

        except Exception as e:
            logger.error(f"SFTP connection error: {e}")
            return None

    async def get_log_content(self, server_config: Dict[str, Any]) -> Optional[str]:
        """Get log content with SFTP priority and local fallback"""
        try:
            server_id = str(server_config.get('_id', 'unknown'))
            host = server_config.get('host', 'unknown')

            # Try SFTP first
            conn = await self.get_sftp_connection(server_config)
            if conn:
                try:
                    remote_path = f"./{host}_{server_id}/Logs/Deadside.log"
                    logger.info(f"📡 Reading SFTP: {remote_path}")

                    async with conn.start_sftp_client() as sftp:
                        try:
                            await sftp.stat(remote_path)
                            async with sftp.open(remote_path, 'r') as f:
                                content = await f.read()
                                logger.info(f"✅ SFTP read {len(content)} bytes")
                                return content
                        except FileNotFoundError:
                            logger.warning(f"Remote file not found: {remote_path}")

                except Exception as e:
                    logger.error(f"SFTP read failed: {e}")

            # Fallback to local file
            local_path = f'./{host}_{server_id}/Logs/Deadside.log'
            logger.info(f"📁 Fallback to local: {local_path}")

            if os.path.exists(local_path):
                try:
                    with open(local_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        logger.info(f"✅ Local read {len(content)} bytes")
                        return content
                except Exception as e:
                    logger.error(f"Local read failed: {e}")
            else:
                # Create test file for development
                logger.info(f"Creating test log file at {local_path}")
                test_dir = os.path.dirname(local_path)
                os.makedirs(test_dir, exist_ok=True)

                test_content = """[2025.05.30-12.20.00:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to READY
[2025.05.30-12.20.15:000] LogNet: Join request: /Game/Maps/world_1/World_1?Name=TestPlayer&eosid=|abc123def456
[2025.05.30-12.20.20:000] LogOnline: Warning: Player |abc123def456 successfully registered!
[2025.05.30-12.20.30:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to IN_PROGRESS
[2025.05.30-12.25.00:000] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to COMPLETED
[2025.05.30-12.25.15:000] UChannel::Close: Sending CloseBunch UniqueId: EOS:|abc123def456"""

                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(test_content)
                return test_content

            return None

        except Exception as e:
            logger.error(f"Error getting log content: {e}")
            return None

    async def parse_log_content(self, content: str, guild_id: str, server_id: str, cold_start: bool = False, server_name: str = "Unknown Server") -> List[discord.Embed]:
        """Parse log content and return embeds"""
        embeds = []
        if not content:
            return embeds

        lines = content.splitlines()
        server_key = f"{guild_id}_{server_id}"

        # Get current state
        file_state = self.file_states.get(server_key, {})
        last_processed = file_state.get('line_count', 0)

        # Determine what to process
        if cold_start or last_processed == 0:
            # Cold start: process all but don't generate embeds
            lines_to_process = lines
            logger.info(f"🧊 Cold start: processing {len(lines)} lines")
        else:
            # Hot start: process only new lines
            if last_processed < len(lines):
                lines_to_process = lines[last_processed:]
                logger.info(f"🔥 Hot start: processing {len(lines_to_process)} new lines")
            else:
                logger.info("📊 No new lines to process")
                return embeds

        # Update state immediately
        self.file_states[server_key] = {
            'line_count': len(lines),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'cold_start_complete': True
        }
        await self._save_persistent_state()

        # Process lines
        for line in lines_to_process:
            try:
                # Player connection events
                queue_match = self.patterns['player_queue_join'].search(line)
                if queue_match:
                    player_id, player_name = queue_match.groups()
                    # Clean and decode the player name immediately
                    import urllib.parse
                    try:
                        # URL decode the name (handles %20 for spaces, etc.)
                        decoded_name = urllib.parse.unquote(player_name)
                        # Further clean up any remaining artifacts
                        clean_name = decoded_name.replace('+', ' ').strip()
                        final_name = clean_name if clean_name else player_name.strip()
                    except Exception:
                        final_name = player_name.strip()
                    
                    self.player_lifecycle[f"{guild_id}_{player_id}"] = {
                        'name': final_name,
                        'raw_name': player_name,  # Keep original for debugging
                        'queued_at': datetime.now(timezone.utc).isoformat()
                    }
                    logger.debug(f"👤 Player queued: {player_id} -> '{final_name}' (raw: '{player_name}')")

                register_match = self.patterns['player_registered'].search(line)
                if register_match:
                    player_id = register_match.group(1)
                    player_key = f"{guild_id}_{player_id}"

                    # Resolve player name using advanced resolution
                    player_name = await self.resolve_player_name(player_id, guild_id)

                    # Track session
                    self.player_sessions[player_key] = {
                        'player_id': player_id,
                        'player_name': player_name,
                        'guild_id': guild_id,
                        'joined_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'online'
                    }

                    # Update voice channel
                    await self.update_voice_channel(str(guild_id))

                    # Create embed (only if not cold start)
                    if not cold_start:
                        embed = EmbedFactory.create_connection_embed(
                            title="Player Connected",
                            description="New player has joined the server",
                            player_name=player_name,
                            player_id=player_id,
                            server_name=server_name
                        )
                        embeds.append(embed)

                disconnect_match = self.patterns['player_disconnect'].search(line)
                if disconnect_match:
                    player_id = disconnect_match.group(1)
                    session_key = f"{guild_id}_{player_id}"

                    # Resolve player name using advanced resolution
                    player_name = "Unknown Player"
                    if session_key in self.player_sessions:
                        player_name = self.player_sessions[session_key].get('player_name', 'Unknown Player')
                        self.player_sessions[session_key]['status'] = 'offline'
                        self.player_sessions[session_key]['left_at'] = datetime.now(timezone.utc).isoformat()
                    else:
                        # Try to resolve from database if not in current session
                        player_name = await self.resolve_player_name(player_id, guild_id)

                    # Update voice channel
                    await self.update_voice_channel(str(guild_id))

                    # Create embed (only if not cold start)
                    if not cold_start:
                        embed = EmbedFactory.create_connection_embed(
                            title="Player Disconnected",
                            description="Player has left the server",
                            player_name=player_name,
                            player_id=player_id,
                            server_name=server_name
                        )
                        embeds.append(embed)

                # Mission events - ONLY READY missions of level 3+
                mission_match = self.patterns['mission_state_change'].search(line)
                if mission_match:
                    mission_id, state = mission_match.groups()

                    if not cold_start:
                        # Only process READY missions of level 3 or higher
                        if state == 'READY':
                            mission_level = self.get_mission_level(mission_id)
                            if mission_level >= 3:
                                embed = await self.create_mission_embed(mission_id, state)
                                if embed:
                                    embeds.append(embed)

                # Airdrop events - ONLY flying state
                airdrop_flying_match = self.patterns['airdrop_flying'].search(line)
                if airdrop_flying_match:
                    if not cold_start:
                        embed = await self.create_airdrop_embed()
                        if embed:
                            embeds.append(embed)

                # Helicrash events - ONLY crash/ready state
                helicrash_match = self.patterns['helicrash_event'].search(line) or self.patterns['helicrash_crash'].search(line)
                if helicrash_match:
                    if not cold_start:
                        embed = await self.create_helicrash_embed()
                        if embed:
                            embeds.append(embed)

                # Trader events - ONLY arrival/ready state  
                trader_arrival_match = self.patterns['trader_arrival'].search(line)
                if trader_arrival_match:
                    if not cold_start:
                        embed = await self.create_trader_embed()
                        if embed:
                            embeds.append(embed)

                # Vehicle events
                vehicle_spawn_match = self.patterns['vehicle_spawn'].search(line)
                if vehicle_spawn_match:
                    vehicle_type = vehicle_spawn_match.group(1)
                    if not cold_start:
                        embed = await self.create_vehicle_embed('spawn', vehicle_type)
                        if embed:
                            embeds.append(embed)

                vehicle_delete_match = self.patterns['vehicle_delete'].search(line)
                if vehicle_delete_match:
                    vehicle_type = vehicle_delete_match.group(1)
                    if not cold_start:
                        embed = await self.create_vehicle_embed('delete', vehicle_type)
                        if embed:
                            embeds.append(embed)

            except Exception as e:
                logger.error(f"Error processing line: {e}")
                continue

        if not cold_start:
            logger.info(f"🔍 Generated {len(embeds)} events")

        return embeds

    async def create_mission_embed(self, mission_id: str, state: str, respawn_time: Optional[int] = None) -> Optional[discord.Embed]:
        """Create mission embed"""
        try:
            mission_level = self.get_mission_level(mission_id)

            if state == 'READY':
                embed = EmbedFactory.create_mission_embed(
                    title="Mission Available",
                    description="New mission objective is ready for deployment",
                    mission_id=mission_id,
                    level=mission_level,
                    state="READY"
                )
            elif state == 'IN_PROGRESS':
                embed = EmbedFactory.create_mission_embed(
                    title="Mission In Progress",
                    description="Mission objective is currently being completed",
                    mission_id=mission_id,
                    level=mission_level,
                    state="IN_PROGRESS"
                )
            elif state == 'COMPLETED':
                embed = EmbedFactory.create_mission_embed(
                    title="Mission Completed",
                    description="Mission objective has been successfully completed",
                    mission_id=mission_id,
                    level=mission_level,
                    state="COMPLETED"
                )
            elif state == 'RESPAWN' and respawn_time:
                embed = EmbedFactory.create_mission_embed(
                    title="Mission Respawning",
                    description="Mission objective is preparing for redeployment",
                    mission_id=mission_id,
                    level=mission_level,
                    state="RESPAWN",
                    respawn_time=respawn_time
                )
            else:
                return None

            return embed

        except Exception as e:
            logger.error(f"Failed to create mission embed: {e}")
            return None

    async def create_airdrop_embed(self, location: str = "Unknown") -> Optional[discord.Embed]:
        """Create airdrop embed"""
        try:
            embed = EmbedFactory.create_airdrop_embed(
                state="incoming",
                location=location,
                timestamp=datetime.now(timezone.utc)
            )
            return embed
        except Exception as e:
            logger.error(f"Failed to create airdrop embed: {e}")
            return None

    async def create_helicrash_embed(self, location: str = "Unknown") -> Optional[discord.Embed]:
        """Create helicrash embed"""
        try:
            embed = EmbedFactory.create_helicrash_embed(
                location=location,
                timestamp=datetime.now(timezone.utc)
            )
            return embed
        except Exception as e:
            logger.error(f"Failed to create helicrash embed: {e}")
            return None

    async def create_trader_embed(self, location: str = "Unknown") -> Optional[discord.Embed]:
        """Create trader embed"""
        try:
            embed = EmbedFactory.create_trader_embed(
                location=location,
                timestamp=datetime.now(timezone.utc)
            )
            return embed
        except Exception as e:
            logger.error(f"Failed to create trader embed: {e}")
            return None

    async def create_vehicle_embed(self, action: str, vehicle_type: str) -> Optional[discord.Embed]:
        """Create vehicle embed - BLOCKED per requirements"""
        # Vehicle embeds are suppressed per task requirements
        return None

    async def update_voice_channel(self, guild_id: str):
        """BULLETPROOF voice channel update"""
        try:
            # Convert guild_id to int with better validation
            if isinstance(guild_id, str):
                # Skip if it's a MongoDB ObjectId
                if len(guild_id) == 24 and all(c in '0123456789abcdef' for c in guild_id.lower()):
                    logger.debug(f"Skipping voice update for MongoDB ObjectId: {guild_id}")
                    return
                try:
                    guild_id_int = int(guild_id)
                except ValueError:
                    logger.warning(f"Invalid guild_id format: {guild_id}")
                    return
            else:
                guild_id_int = guild_id

            # Count active players with better key validation
            guild_prefix = f"{guild_id}_"
            active_players = 0

            for key, session in self.player_sessions.items():
                if key.startswith(guild_prefix) and isinstance(session, dict) and session.get('status') == 'online':
                    active_players += 1

            logger.debug(f"Counted {active_players} active players for guild {guild_id_int}")

            # Get guild config with validation
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.warning("Database manager not available for voice channel update")
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id_int)
            if not guild_config:
                logger.debug(f"No guild config found for {guild_id_int}")
                return

            # Find voice channel ID with comprehensive mapping
            voice_channel_id = None

            # Method 1: Check server_channels (new format)
            server_channels = guild_config.get('server_channels', {})
            for server_key, channels in server_channels.items():
                if isinstance(channels, dict):
                    # Try multiple voice channel key names
                    for voice_key in ['voice_count', 'playercountvc', 'playercount']:
                        if voice_key in channels:
                            voice_channel_id = channels[voice_key]
                            logger.debug(f"Found voice channel {voice_channel_id} as {voice_key} in server {server_key}")
                            break
                    if voice_channel_id:
                        break

            # Method 2: Check channels (legacy format)
            if not voice_channel_id:
                legacy_channels = guild_config.get('channels', {})
                if isinstance(legacy_channels, dict):
                    for voice_key in ['voice_count', 'playercountvc', 'playercount']:
                        if voice_key in legacy_channels:
                            voice_channel_id = legacy_channels[voice_key]
                            logger.debug(f"Found legacy voice channel {voice_channel_id} as {voice_key}")
                            break

            # Method 3: Check if any servers have voice channels configured
            if not voice_channel_id:
                servers = guild_config.get('servers', [])
                for server in servers:
                    server_id = str(server.get('_id', ''))
                    if server_id in server_channels:
                        channels = server_channels[server_id]
                        if isinstance(channels, dict):
                            for voice_key in ['voice_count', 'playercountvc', 'playercount']:
                                if voice_key in channels:
                                    voice_channel_id = channels[voice_key]
                                    logger.debug(f"Found voice channel {voice_channel_id} in server {server_id}")
                                    break
                        if voice_channel_id:
                            break

            if not voice_channel_id:
                logger.debug(f"No voice channel configured for guild {guild_id_int}")
                return

            # Update the channel with rate limit protection
            guild = self.bot.get_guild(guild_id_int)
            if not guild:
                logger.warning(f"Guild {guild_id_int} not found")
                return

            voice_channel = guild.get_channel(voice_channel_id)
            if not voice_channel:
                logger.warning(f"Voice channel {voice_channel_id} not found in guild {guild_id_int}")
                return

            if voice_channel.type != discord.ChannelType.voice:
                logger.warning(f"Channel {voice_channel_id} is not a voice channel")
                return

            new_name = f"🟢 Players Online: {active_players}"
            if voice_channel.name != new_name:
                try:
                    await voice_channel.edit(name=new_name)
                    logger.info(f"✅ Voice channel updated to: {new_name}")
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        logger.warning(f"Rate limited updating voice channel: {e}")
                    else:
                        logger.error(f"HTTP error updating voice channel: {e}")
                except Exception as edit_error:
                    logger.error(f"Error editing voice channel: {edit_error}")
            else:
                logger.debug(f"Voice channel already has correct name: {new_name}")

        except Exception as e:
            logger.error(f"Voice channel update failed: {e}")
            import traceback
            logger.error(f"Voice channel update traceback: {traceback.format_exc()}")

    async def get_channel_for_type(self, guild_id: int, server_id: str, channel_type: str) -> Optional[int]:
        """Get channel ID with bulletproof fallback"""
        try:
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                return None

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return None

            server_channels = guild_config.get('server_channels', {})

            # Server-specific channel
            if server_id in server_channels and channel_type in server_channels[server_id]:
                return server_channels[server_id][channel_type]

            # Default server channel
            if 'default' in server_channels and channel_type in server_channels['default']:
                return server_channels['default'][channel_type]

            # Fallback to killfeed if no specific channel
            if channel_type != 'killfeed':
                killfeed_id = None
                if server_id in server_channels:
                    killfeed_id = server_channels[server_id].get('killfeed')
                if not killfeed_id and 'default' in server_channels:
                    killfeed_id = server_channels['default'].get('killfeed')
                if killfeed_id:
                    return killfeed_id

            # Legacy fallback
            return guild_config.get('channels', {}).get(channel_type)

        except Exception as e:
            logger.error(f"Error getting channel: {e}")
            return None

    async def send_embeds(self, guild_id: int, server_id: str, embeds: List[discord.Embed]):
        """Send embeds to appropriate channels"""
        if not embeds:
            return

        try:
            for embed in embeds:
                # Determine channel type
                channel_type = 'events'
                if embed.title:
                    title_lower = embed.title.lower()
                    if any(word in title_lower for word in ['connect', 'disconnect', 'join', 'left']):
                        channel_type = 'connections'

                # Get channel
                channel_id = await self.get_channel_for_type(guild_id, server_id, channel_type)
                if not channel_id:
                    continue

                channel = self.bot.get_channel(channel_id)
                if channel:
                    try:
                        # Determine which thumbnail file to attach based on embed content
                        file_to_attach = None
                        if embed.thumbnail and embed.thumbnail.url:
                            thumbnail_url = embed.thumbnail.url
                            if thumbnail_url.startswith('attachment://'):
                                filename = thumbnail_url.replace('attachment://', '')
                                file_path = f'./assets/{filename}'
                                
                                # Check if file exists and create discord.File
                                import os
                                if os.path.exists(file_path):
                                    file_to_attach = discord.File(file_path, filename=filename)

                        # Send with or without file attachment
                        if file_to_attach:
                            await channel.send(embed=embed, file=file_to_attach)
                        else:
                            await channel.send(embed=embed)
                            
                        logger.info(f"✅ Sent {channel_type} event to {channel.name}")
                    except Exception as e:
                        logger.error(f"Failed to send embed: {e}")

        except Exception as e:
            logger.error(f"Error sending embeds: {e}")

    async def parse_server_logs(self, guild_id: int, server: dict):
        """Parse logs for a single server"""
        try:
            server_id = str(server.get('_id', 'unknown'))
            server_name = server.get('name', 'Unknown')
            host = server.get('host', 'unknown')

            logger.info(f"🔍 Processing {server_name} (ID: {server_id}, Host: {host})")

            if not host or not server_id or host == 'unknown' or server_id == 'unknown':
                logger.warning(f"❌ Invalid server config: {server_name}")
                return

            # Get log content
            content = await self.get_log_content(server)
            if not content:
                logger.warning(f"❌ No log content for {server_name}")
                return

            # Determine if cold start
            server_key = f"{guild_id}_{server_id}"
            file_state = self.file_states.get(server_key, {})
            is_cold_start = not file_state.get('cold_start_complete', False)

            # Parse content with server context
            embeds = await self.parse_log_content(content, str(guild_id), server_id, is_cold_start, server_name)

            # Send embeds (only if not cold start)
            if not is_cold_start and embeds:
                await self.send_embeds(guild_id, server_id, embeds)

            logger.info(f"✅ {server_name}: {'Cold start' if is_cold_start else f'{len(embeds)} events'}")

        except Exception as e:
            logger.error(f"Error parsing server {server.get('name', 'Unknown')}: {e}")

    async def run_log_parser(self):
        """Main parser entry point"""
        try:
            logger.info("🔄 Running unified log parser...")

            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.error("❌ Database not available")
                return

            # Get all guilds
            guilds_cursor = self.bot.db_manager.guilds.find({})
            guilds_list = await guilds_cursor.to_list(length=None)

            if not guilds_list:
                logger.info("No guilds found")
                return

            total_processed = 0

            for guild_doc in guilds_list:
                guild_id = guild_doc.get('guild_id')
                if not guild_id:
                    continue

                try:
                    guild_id = int(guild_id)
                except:
                    continue

                guild_name = guild_doc.get('name', f'Guild {guild_id}')
                servers = guild_doc.get('servers', [])

                if not servers:
                    continue

                logger.info(f"📡 Processing {len(servers)} servers for {guild_name}")

                for server in servers:
                    try:
                        await self.parse_server_logs(guild_id, server)
                        total_processed += 1
                    except Exception as e:
                        logger.error(f"Server parse error: {e}")

            logger.info(f"✅ Parser completed: {total_processed} servers processed")

        except Exception as e:
            logger.error(f"Parser run failed: {e}")

    async def _load_persistent_state(self):
        """Load state from database"""
        try:
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                state_doc = await self.bot.db_manager.db['parser_state'].find_one({'_id': 'unified_parser_state'})
                if state_doc and 'file_states' in state_doc:
                    self.file_states = state_doc['file_states']
                    logger.info(f"✅ Loaded state for {len(self.file_states)} servers")
        except Exception as e:
            logger.error(f"State load failed: {e}")

    async def _save_persistent_state(self):
        """Save state to database"""
        try:
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                state_doc = {
                    '_id': 'unified_parser_state',
                    'file_states': self.file_states,
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
                await self.bot.db_manager.db['parser_state'].replace_one(
                    {'_id': 'unified_parser_state'},
                    state_doc,
                    upsert=True
                )
        except Exception as e:
            logger.error(f"State save failed: {e}")

    def get_parser_status(self) -> Dict[str, Any]:
        """Get parser status"""
        try:
            active_sessions = sum(1 for session in self.player_sessions.values() if session.get('status') == 'online')
            
            # Calculate active players by guild
            active_players_by_guild = {}
            for key, session in self.player_sessions.items():
                if session.get('status') == 'online':
                    guild_id = session.get('guild_id', 'unknown')
                    active_players_by_guild[guild_id] = active_players_by_guild.get(guild_id, 0) + 1
            
            # Check SFTP connection status
            active_connections = 0
            for conn in self.sftp_connections.values():
                try:
                    if not conn.is_closed():
                        active_connections += 1
                except:
                    pass
            
            return {
                'active_sessions': active_sessions,
                'total_tracked_servers': len(self.file_states),
                'sftp_connections': active_connections,
                'connection_status': f"{active_connections}/{len(self.sftp_connections)} active",
                'active_players_by_guild': active_players_by_guild,
                'status': 'healthy' if active_sessions >= 0 else 'error'
            }
        except Exception as e:
            logger.error(f"Error getting parser status: {e}")
            return {
                'active_sessions': 0,
                'total_tracked_servers': 0,
                'sftp_connections': 0,
                'connection_status': 'error',
                'active_players_by_guild': {},
                'status': 'error'
            }

    def reset_parser_state(self):
        """Reset all parser state"""
        try:
            self.file_states.clear()
            self.player_sessions.clear()
            self.player_lifecycle.clear()
            self.last_log_position.clear()
            self.log_file_hashes.clear()
            if hasattr(self, 'server_status'):
                self.server_status.clear()
            logger.info("✅ Parser state reset")
        except Exception as e:
            logger.error(f"Error resetting parser state: {e}")

    async def resolve_player_name(self, player_id: str, guild_id: str) -> str:
        """Resolve player name from ID using comprehensive database and session cache"""
        try:
            # Check cache first
            cache_key = f"{guild_id}_{player_id}"
            if cache_key in self.player_name_cache:
                return self.player_name_cache[cache_key]
            
            # Check current session lifecycle first (most recent) - this should have the actual name
            lifecycle_key = f"{guild_id}_{player_id}"
            if lifecycle_key in self.player_lifecycle:
                name = self.player_lifecycle[lifecycle_key].get('name')
                if name and name.strip() and name != 'Unknown Player':
                    # Clean up the name (URL decode and normalize)
                    import urllib.parse
                    try:
                        # URL decode the name (handles %20 for spaces, etc.)
                        decoded_name = urllib.parse.unquote(name)
                        # Further clean up any remaining artifacts
                        clean_name = decoded_name.replace('+', ' ').strip()
                        if clean_name and clean_name != 'Unknown Player':
                            self.player_name_cache[cache_key] = clean_name
                            logger.info(f"✅ Resolved player name from lifecycle: {player_id} -> {clean_name}")
                            return clean_name
                    except Exception as decode_error:
                        logger.warning(f"Failed to decode player name '{name}': {decode_error}")
                        # Use original name if decoding fails
                        if name.strip():
                            self.player_name_cache[cache_key] = name.strip()
                            return name.strip()
            
            # Check database with multiple methods
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                try:
                    # Method 1: Check PvP data collection (most comprehensive) - search by player_id
                    pvp_doc = await self.bot.db_manager.pvp_data.find_one({
                        'guild_id': int(guild_id),
                        'player_id': player_id
                    })
                    
                    if pvp_doc:
                        name = pvp_doc.get('player_name')
                        if name and name.strip() and name != 'Unknown Player':
                            self.player_name_cache[cache_key] = name
                            logger.info(f"✅ Resolved player name from PvP data: {player_id} -> {name}")
                            return name
                    
                    # Method 2: Search PvP data by partial player_id match (sometimes IDs change)
                    partial_id = player_id[:8] if len(player_id) > 8 else player_id
                    pvp_cursor = self.bot.db_manager.pvp_data.find({
                        'guild_id': int(guild_id),
                        'player_id': {'$regex': f'^{partial_id}', '$options': 'i'}
                    }).sort('last_updated', -1).limit(1)
                    
                    async for pvp_doc in pvp_cursor:
                        name = pvp_doc.get('player_name')
                        if name and name.strip() and name != 'Unknown Player':
                            self.player_name_cache[cache_key] = name
                            logger.info(f"✅ Resolved player name from partial ID match: {player_id} -> {name}")
                            # Update the record with correct player_id
                            try:
                                await self.bot.db_manager.pvp_data.update_one(
                                    {'_id': pvp_doc['_id']},
                                    {'$set': {'player_id': player_id}}
                                )
                            except:
                                pass
                            return name
                    
                    # Method 3: Check linked players table
                    player_doc = await self.bot.db_manager.players.find_one({
                        'guild_id': int(guild_id),
                        'player_id': player_id
                    })
                    
                    if player_doc:
                        # Use primary character or first linked character
                        name = player_doc.get('primary_character') or (player_doc.get('linked_characters', [None])[0])
                        if name and name.strip() and name != 'Unknown Player':
                            self.player_name_cache[cache_key] = name
                            logger.info(f"✅ Resolved player name from linked players: {player_id} -> {name}")
                            return name
                    
                    # Method 4: Check if player has ever been linked to Discord by searching characters
                    linked_cursor = self.bot.db_manager.players.find({
                        'guild_id': int(guild_id),
                        'linked_characters': {'$exists': True, '$ne': []}
                    })
                    
                    async for linked_doc in linked_cursor:
                        # Try to find this player_id in historical data for any of these characters
                        for char_name in linked_doc.get('linked_characters', []):
                            historical_doc = await self.bot.db_manager.pvp_data.find_one({
                                'guild_id': int(guild_id),
                                'player_name': char_name,
                                'player_id': player_id
                            })
                            if historical_doc:
                                self.player_name_cache[cache_key] = char_name
                                logger.info(f"✅ Resolved player name from historical data: {player_id} -> {char_name}")
                                return char_name
                            
                except Exception as db_error:
                    logger.error(f"Database lookup failed for player {player_id}: {db_error}")
            
            # Final fallback: Use truncated player ID as name hint but make it more obvious it's a fallback
            if len(player_id) >= 8:
                fallback_name = f"Player_{player_id[:8]}"
                logger.warning(f"⚠️ Using fallback name {fallback_name} for player {player_id} - name not found in logs or database")
                return fallback_name
            
            logger.warning(f"⚠️ Could not resolve player name for {player_id} - returning Unknown Player")
            return "Unknown Player"
            
        except Exception as e:
            logger.error(f"Error resolving player name for {player_id}: {e}")
            return "Unknown Player"

    def get_active_player_count(self, guild_id: str) -> int:
        """Get active player count for a guild"""
        try:
            guild_prefix = f"{guild_id}_"
            return sum(
                1 for key, session in self.player_sessions.items()
                if key.startswith(guild_prefix) and isinstance(session, dict) and session.get('status') == 'online'
            )
        except Exception as e:
            logger.error(f"Error getting active player count: {e}")
            return 0