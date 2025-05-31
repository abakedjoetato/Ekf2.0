"""
Emerald's Killfeed - Embed Factory System
Professional, esports-adjacent dark-themed formatting for all embeds
"""

import discord
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple


class EmbedFactory:
    """
    EMERALD EMBED FACTORY
    Professional, esports-adjacent dark-themed formatting
    No emojis, clean field separation, consistent theming
    """

    # Emerald brand colors
    COLORS = {
        'success': 0x2ECC71,    # Green
        'warning': 0xF39C12,    # Orange
        'error': 0xE74C3C,      # Red
        'info': 0x3498DB,       # Blue
        'neutral': 0x95A5A6,    # Gray
        'emerald': 0x00D4AA,    # Emerald brand
        'mission': 0x9B59B6,    # Purple
        'connection': 0x2ECC71,  # Green
        'disconnection': 0xE74C3C,  # Red
        'airdrop': 0xF39C12,    # Orange
        'trader': 0xFFD700,     # Gold
        'helicrash': 0xE67E22,  # Dark orange
    }

    # Asset mappings for thumbnails
    ASSETS = {
        'mission': 'Mission.png',
        'connection': 'Connections.png',
        'airdrop': 'Airdrop.png',
        'trader': 'Trader.png',
        'helicrash': 'Helicrash.png',
        'vehicle': 'Vehicle.png',
        'killfeed': 'Killfeed.png',
        'main': 'main.png',
        'bounty': 'Bounty.png',
        'faction': 'Faction.png',
        'economy': 'Gamble.png'
    }

    # Mission name mappings
    MISSION_MAPPINGS = {
        'GA_Airport_mis_01_SFPSACMission': 'Airport Terminal Raid',
        'GA_Airport_mis_02_SFPSACMission': 'Airport Hangar Assault',
        'GA_Airport_mis_03_SFPSACMission': 'Airport Control Tower',
        'GA_Airport_mis_04_SFPSACMission': 'Airport Cargo Complex',
        'GA_Military_02_Mis1': 'Military Base Compound',
        'GA_Military_03_Mis_01': 'Military Armory Raid',
        'GA_Military_04_Mis1': 'Military Command Center',
        'GA_Military_04_Mis_2': 'Military Outpost Alpha',
        'GA_Beregovoy_Mis1': 'Beregovoy Settlement',
        'GA_Settle_05_ChernyLog_Mis1': 'Cherny Log Outpost',
        'GA_Ind_01_m1': 'Industrial Complex Alpha',
        'GA_Ind_02_Mis_1': 'Industrial Complex Beta',
        'GA_KhimMash_Mis_01': 'Chemical Plant Primary',
        'GA_KhimMash_Mis_02': 'Chemical Plant Secondary',
        'GA_Bunker_01_Mis1': 'Underground Bunker',
        'GA_Sawmill_01_Mis1': 'Sawmill Operations',
        'GA_Settle_09_Mis_1': 'Settlement Omega',
        'GA_PromZone_6_Mis_1': 'Industrial Zone Six',
        'GA_PromZone_Mis_01': 'Industrial Zone Alpha',
        'GA_PromZone_Mis_02': 'Industrial Zone Beta',
        'GA_Kamensk_Ind_3_Mis_1': 'Kamensk Industrial',
        'GA_Kamensk_Mis_1': 'Kamensk City Center',
        'GA_Kamensk_Mis_2': 'Kamensk Residential',
        'GA_Kamensk_Mis_3': 'Kamensk Commercial',
        'GA_Krasnoe_Mis_1': 'Krasnoe Township',
        'GA_Vostok_Mis_1': 'Vostok Settlement',
        'GA_Lighthouse_02_Mis1': 'Lighthouse Station',
        'GA_Elevator_Mis_1': 'Elevator Complex Alpha',
        'GA_Elevator_Mis_2': 'Elevator Complex Beta',
        'GA_Sawmill_02_1_Mis1': 'Sawmill Secondary',
        'GA_Sawmill_03_Mis_01': 'Sawmill Tertiary',
        'GA_Bochki_Mis_1': 'Barrel Storage Facility',
        'GA_Dubovoe_0_Mis_1': 'Dubovoe Resource Site',
    }

    @classmethod
    def normalize_mission_name(cls, mission_id: str) -> str:
        """Convert mission ID to readable name"""
        if mission_id in cls.MISSION_MAPPINGS:
            return cls.MISSION_MAPPINGS[mission_id]

        # Generate fallback name based on patterns
        if '_Airport_' in mission_id:
            return f"Airport Sector ({mission_id.split('_')[-1]})"
        elif '_Military_' in mission_id:
            return f"Military Operation ({mission_id.split('_')[-1]})"
        elif '_Ind_' in mission_id or '_PromZone_' in mission_id:
            return f"Industrial Raid ({mission_id.split('_')[-1]})"
        elif '_KhimMash_' in mission_id:
            return f"Chemical Plant ({mission_id.split('_')[-1]})"
        elif '_Bunker_' in mission_id:
            return f"Bunker Complex ({mission_id.split('_')[-1]})"
        elif '_Sawmill_' in mission_id:
            return f"Sawmill Operation ({mission_id.split('_')[-1]})"
        else:
            # Extract readable parts
            parts = mission_id.replace('GA_', '').replace('_Mis', '').replace('_mis', '').split('_')
            readable_parts = [part.capitalize() for part in parts if part.isalpha()]
            if readable_parts:
                return f"{' '.join(readable_parts)} Operation"
            else:
                return f"Operation {mission_id.split('_')[-1] if '_' in mission_id else mission_id}"

    @classmethod
    def get_mission_level(cls, mission_id: str) -> int:
        """Determine mission difficulty level"""
        if any(keyword in mission_id.lower() for keyword in ['military', 'bunker', 'khimmash']):
            return 5  # High tier
        elif any(keyword in mission_id.lower() for keyword in ['airport', 'promzone', 'kamensk']):
            return 4  # High-medium tier
        elif any(keyword in mission_id.lower() for keyword in ['ind_', 'industrial']):
            return 3  # Medium tier
        elif any(keyword in mission_id.lower() for keyword in ['sawmill', 'lighthouse', 'elevator']):
            return 2  # Low-medium tier
        else:
            return 1  # Low tier

    @classmethod
    def build_base_embed(cls, title: str, description: str, color: int, thumbnail: Optional[str] = None) -> discord.Embed:
        """Build base embed with Emerald theming"""
        embed = discord.Embed(
            title=title,
            description=description,
            color=color,
            timestamp=datetime.now(timezone.utc)
        )

        # Don't set thumbnail here - it will be set in the build method with proper file attachment
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed

    @classmethod
    def create_mission_embed(cls, title: str, description: str, mission_id: str, level: int, state: str, respawn_time: Optional[int] = None, **kwargs) -> discord.Embed:
        """Create mission embed"""
        color = cls.COLORS['mission']
        if state == 'READY':
            color = cls.COLORS['success']
        elif state == 'IN_PROGRESS':
            color = cls.COLORS['warning']
        elif state == 'COMPLETED':
            color = cls.COLORS['info']
        elif state == 'RESPAWN':
            color = cls.COLORS['neutral']

        embed = cls.build_base_embed(title, description, color, 'mission')

        mission_name = cls.normalize_mission_name(mission_id)
        embed.add_field(name="Mission", value=mission_name, inline=True)
        embed.add_field(name="Difficulty", value=f"Level {level}", inline=True)
        embed.add_field(name="Status", value=state.replace('_', ' ').title(), inline=True)

        if respawn_time:
            embed.add_field(name="Respawn Timer", value=f"{respawn_time} seconds", inline=False)

        return embed

    @classmethod
    def create_connection_embed(cls, title: str, description: str, player_name: str, player_id: str, **kwargs) -> discord.Embed:
        """Create player connection embed"""
        color = cls.COLORS['connection'] if 'Connected' in title else cls.COLORS['disconnection']

        embed = cls.build_base_embed(title, description, color, 'connection')
        embed.add_field(name="Player", value=player_name, inline=True)

        # Add server info if available
        server_info = kwargs.get('server_name', 'Unknown Server')
        embed.add_field(name="Server", value=server_info, inline=True)

        return embed

    @classmethod
    def create_airdrop_embed(cls, state: str, location: str = "Unknown", timestamp: Optional[datetime] = None, **kwargs) -> discord.Embed:
        """Create airdrop embed"""
        titles = [
            "Airdrop Incoming",
            "Supply Drop Detected",
            "Cargo Drop Inbound"
        ]

        descriptions = [
            f"Supply airdrop incoming at {location}",
            f"Cargo drop detected at {location}",
            f"Military supply drop at {location}"
        ]

        title = random.choice(titles)
        description = random.choice(descriptions)

        embed = cls.build_base_embed(title, description, cls.COLORS['airdrop'], 'airdrop')
        embed.add_field(name="Location", value=location, inline=True)
        embed.add_field(name="Status", value=state.title(), inline=True)

        return embed

    @classmethod
    def create_helicrash_embed(cls, location: str = "Unknown", timestamp: Optional[datetime] = None, **kwargs) -> discord.Embed:
        """Create helicrash embed"""
        titles = [
            "Helicopter Crash Detected",
            "Aircraft Down",
            "Crash Site Located"
        ]

        descriptions = [
            f"Helicopter crash site discovered at {location}",
            f"Aircraft wreckage detected at {location}",
            f"Crash site confirmed at {location}"
        ]

        title = random.choice(titles)
        description = random.choice(descriptions)

        embed = cls.build_base_embed(title, description, cls.COLORS['helicrash'], 'helicrash')
        embed.add_field(name="Location", value=location, inline=True)
        embed.add_field(name="Status", value="Active", inline=True)

        return embed

    @classmethod
    def create_trader_embed(cls, location: str = "Unknown", timestamp: Optional[datetime] = None, **kwargs) -> discord.Embed:
        """Create trader embed"""
        titles = [
            "Trader Arrived",
            "Merchant Available",
            "Trading Post Active"
        ]

        descriptions = [
            f"Trader has arrived at {location}",
            f"Merchant available for trading at {location}",
            f"Trading post established at {location}"
        ]

        title = random.choice(titles)
        description = random.choice(descriptions)

        embed = cls.build_base_embed(title, description, cls.COLORS['trader'], 'trader')
        embed.add_field(name="Location", value=location, inline=True)
        embed.add_field(name="Status", value="Available", inline=True)

        return embed

    # VEHICLE EMBEDS BLOCKED - These methods exist but return None to suppress output
    @classmethod
    def create_vehicle_embed(cls, action: str, vehicle_type: str, **kwargs) -> Optional[discord.Embed]:
        """Create vehicle embed - BLOCKED per requirements"""
        return None

    @classmethod
    def create_killfeed_embed(cls, killer_name: str, victim_name: str, weapon: str, distance: Optional[float] = None, **kwargs) -> discord.Embed:
        """Create killfeed embed"""
        embed = cls.build_base_embed(
            "Player Eliminated",
            f"{killer_name} eliminated {victim_name}",
            cls.COLORS['error'],
            'killfeed'
        )

        embed.add_field(name="Killer", value=killer_name, inline=True)
        embed.add_field(name="Victim", value=victim_name, inline=True)
        embed.add_field(name="Weapon", value=weapon, inline=True)

        if distance:
            embed.add_field(name="Distance", value=f"{distance:.1f}m", inline=True)

        return embed

    @classmethod
    def create_stats_embed(cls, title: str, description: str, stats_data: Dict[str, Any], **kwargs) -> discord.Embed:
        """Create statistics embed"""
        embed = cls.build_base_embed(title, description, cls.COLORS['info'], 'main')

        for key, value in stats_data.items():
            embed.add_field(name=key, value=str(value), inline=True)

        return embed

    @classmethod
    def create_leaderboard_embed(cls, title: str, leaderboard_data: List[Dict[str, Any]], **kwargs) -> discord.Embed:
        """Create leaderboard embed"""
        embed = cls.build_base_embed(title, "Top performers on the server", cls.COLORS['emerald'], 'main')

        for i, entry in enumerate(leaderboard_data[:10], 1):
            player_name = entry.get('player_name', 'Unknown')
            value = entry.get('value', 0)
            embed.add_field(name=f"{i}. {player_name}", value=str(value), inline=False)

        return embed

    @classmethod
    def create_economy_embed(cls, title: str, description: str, amount: Optional[int] = None, **kwargs) -> discord.Embed:
        """Create economy embed"""
        embed = cls.build_base_embed(title, description, cls.COLORS['emerald'], 'economy')

        if amount is not None:
            embed.add_field(name="Amount", value=f"{amount:,} credits", inline=True)

        return embed

    @classmethod
    def create_bounty_embed(cls, title: str, description: str, target: str, amount: int, **kwargs) -> discord.Embed:
        """Create bounty embed"""
        embed = cls.build_base_embed(title, description, cls.COLORS['error'], 'bounty')
        embed.add_field(name="Target", value=target, inline=True)
        embed.add_field(name="Bounty", value=f"{amount:,} credits", inline=True)

        return embed

    @classmethod
    def create_faction_embed(cls, title: str, description: str, faction_name: str, **kwargs) -> discord.Embed:
        """Create faction embed"""
        embed = cls.build_base_embed(title, description, cls.COLORS['mission'], 'faction')
        embed.add_field(name="Faction", value=faction_name, inline=True)

        return embed

    @classmethod
    def create_suicide_embed(cls, player_name: str, cause: str, **kwargs) -> discord.Embed:
        """Create suicide embed"""
        embed = cls.build_base_embed(
            "Player Death",
            f"{player_name} died by {cause}",
            cls.COLORS['neutral'],
            'main'
        )
        embed.add_field(name="Player", value=player_name, inline=True)
        embed.add_field(name="Cause", value=cause, inline=True)
        return embed

    @classmethod
    def create_fall_embed(cls, player_name: str, **kwargs) -> discord.Embed:
        """Create falling death embed"""
        embed = cls.build_base_embed(
            "Fall Death",
            f"{player_name} died from falling",
            cls.COLORS['warning'],
            'main'
        )
        embed.add_field(name="Player", value=player_name, inline=True)
        embed.add_field(name="Cause", value="Falling Damage", inline=True)
        return embed

    # The build method is replaced with the new connection embed logic and other modifications
    @staticmethod
    async def build(embed_type: str, data: Dict[str, Any]) -> Tuple[discord.Embed, Optional[discord.File]]:
        """
        Build embed with attachment based on type
        """
        try:
            embed = None
            file_attachment = None

            if embed_type == 'connection':
                embed = discord.Embed(
                    title=data.get('title', 'Connection Event'),
                    description=data.get('description', 'Player connection status changed'),
                    color=EmbedFactory.COLORS['primary'],
                    timestamp=datetime.now(timezone.utc)
                )

                # Add connection fields
                embed.add_field(
                    name="Player Name",
                    value=data.get('player_name', 'Unknown'),
                    inline=True
                )

                embed.add_field(
                    name="Platform",
                    value=data.get('platform', 'Unknown'),
                    inline=True
                )

                embed.add_field(
                    name="Server Name", 
                    value=data.get('server_name', 'Unknown Server'),
                    inline=True
                )

                file_attachment = discord.File('assets/Connections.png', filename='connections.png')
                embed.set_thumbnail(url='attachment://connections.png')

            elif embed_type == 'killfeed':
                embed = EmbedFactory.create_killfeed_embed(
                    killer=data.get('killer', 'Unknown'),
                    victim=data.get('victim', 'Unknown'),
                    weapon=data.get('weapon', 'Unknown'),
                    distance=data.get('distance', 0),
                    headshot=data.get('headshot', False),
                    suicide=data.get('suicide', False)
                )

                # Add killfeed icon
                if data.get('suicide', False):
                    file_attachment = discord.File('assets/Suicide.png', filename='suicide.png')
                    embed.set_thumbnail(url='attachment://suicide.png')
                else:
                    file_attachment = discord.File('assets/Killfeed.png', filename='killfeed.png')
                    embed.set_thumbnail(url='attachment://killfeed.png')

            elif embed_type == 'mission':
                embed = EmbedFactory.create_mission_embed(
                    title=data.get('title', 'Mission Update'),
                    description=data.get('description', 'Mission status changed'),
                    mission_id=data.get('mission_id', ''),
                    level=data.get('level', 1),
                    state=data.get('state', 'UNKNOWN')
                )

                file_attachment = discord.File('assets/Mission.png', filename='mission.png')
                embed.set_thumbnail(url='attachment://mission.png')

            elif embed_type == 'airdrop':
                embed = EmbedFactory.create_airdrop_embed(
                    state=data.get('state', 'incoming'),
                    location=data.get('location', 'Unknown'),
                    timestamp=datetime.now(timezone.utc)
                )

                file_attachment = discord.File('assets/Airdrop.png', filename='airdrop.png')
                embed.set_thumbnail(url='attachment://airdrop.png')

            elif embed_type == 'helicrash':
                embed = EmbedFactory.create_helicrash_embed(
                    location=data.get('location', 'Unknown'),
                    timestamp=datetime.now(timezone.utc)
                )

                file_attachment = discord.File('assets/Helicrash.png', filename='helicrash.png')
                embed.set_thumbnail(url='attachment://helicrash.png')

            elif embed_type == 'trader':
                embed = EmbedFactory.create_trader_embed(
                    location=data.get('location', 'Unknown'),
                    timestamp=datetime.now(timezone.utc)
                )

                file_attachment = discord.File('assets/Trader.png', filename='trader.png')
                embed.set_thumbnail(url='attachment://trader.png')

            # Default fallback
            if not embed:
                embed = discord.Embed(
                    title="Event Update",
                    description="An event has occurred",
                    color=EmbedFactory.COLORS['primary'],
                    timestamp=datetime.now(timezone.utc)
                )

            # Set consistent footer
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            return embed, file_attachment

        except Exception as e:
            logger.error(f"Error building embed: {e}")
            # Return basic embed on error
            embed = discord.Embed(
                title="System Update",
                description="An event has occurred",
                color=EmbedFactory.COLORS['primary'],
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            return embed, None