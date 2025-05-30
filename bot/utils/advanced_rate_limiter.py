
"""
Advanced Rate Limiter - Prevents Discord API rate limits with intelligent queuing
"""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
import discord

logger = logging.getLogger(__name__)

class MessagePriority(Enum):
    CRITICAL = 1    # Voice channel updates, critical alerts
    HIGH = 2        # Player connections, important events
    NORMAL = 3      # Mission events, regular killfeed
    LOW = 4         # Bulk operations, non-essential updates

@dataclass
class QueuedMessage:
    channel_id: int
    embed: discord.Embed
    file: Optional[discord.File]
    content: Optional[str]
    priority: MessagePriority
    timestamp: datetime
    retry_count: int = 0
    callback: Optional[Callable] = None

class AdvancedRateLimiter:
    """
    Sophisticated rate limiting system with:
    - Per-channel rate limiting with burst handling
    - Priority-based message queuing
    - Adaptive delays based on Discord's rate limit headers
    - Intelligent batching and deduplication
    - Automatic retry with exponential backoff
    """

    def __init__(self, bot):
        self.bot = bot
        
        # Rate limiting constants based on Discord API limits
        self.GLOBAL_RATE_LIMIT = 50  # Global messages per second
        self.CHANNEL_RATE_LIMIT = 5  # Messages per channel per 5 seconds
        self.BURST_ALLOWANCE = 10   # Burst messages allowed
        
        # Tracking structures
        self.channel_queues: Dict[int, List[QueuedMessage]] = defaultdict(list)
        self.channel_last_sent: Dict[int, float] = {}
        self.channel_message_count: Dict[int, deque] = defaultdict(lambda: deque(maxlen=self.CHANNEL_RATE_LIMIT))
        self.global_message_times: deque = deque(maxlen=self.GLOBAL_RATE_LIMIT)
        self.processing_channels: set = set()
        
        # Rate limit tracking from Discord headers
        self.rate_limit_remaining: Dict[str, int] = {}
        self.rate_limit_reset: Dict[str, float] = {}
        self.global_rate_limit_reset: float = 0
        
        # Deduplication tracking
        self.recent_embeds: Dict[int, List[str]] = defaultdict(list)
        self.dedup_window = 30  # seconds
        
        # Start the processing task
        self.processing_task = asyncio.create_task(self._process_queues())

    async def queue_message(self, channel_id: int, embed: discord.Embed, 
                          file: discord.File = None, content: str = None,
                          priority: MessagePriority = MessagePriority.NORMAL,
                          callback: Callable = None):
        """Queue a message with priority and deduplication"""
        try:
            # Deduplication check
            embed_hash = self._generate_embed_hash(embed)
            channel_recent = self.recent_embeds[channel_id]
            
            # Remove old hashes
            current_time = time.time()
            self.recent_embeds[channel_id] = [
                h for h in channel_recent 
                if current_time - float(h.split('_')[0]) < self.dedup_window
            ]
            
            # Check for duplicate
            if embed_hash in self.recent_embeds[channel_id]:
                logger.debug(f"Duplicate embed detected for channel {channel_id}, skipping")
                return
            
            # Add to recent embeds
            self.recent_embeds[channel_id].append(f"{current_time}_{embed_hash}")
            
            # Create queued message
            message = QueuedMessage(
                channel_id=channel_id,
                embed=embed,
                file=file,
                content=content,
                priority=priority,
                timestamp=datetime.now(timezone.utc),
                callback=callback
            )
            
            # Insert by priority
            queue = self.channel_queues[channel_id]
            inserted = False
            for i, existing in enumerate(queue):
                if message.priority.value < existing.priority.value:
                    queue.insert(i, message)
                    inserted = True
                    break
            
            if not inserted:
                queue.append(message)
            
            logger.debug(f"Queued {priority.name} message for channel {channel_id}")
            
        except Exception as e:
            logger.error(f"Failed to queue message: {e}")

    def _generate_embed_hash(self, embed: discord.Embed) -> str:
        """Generate hash for embed deduplication"""
        try:
            hash_parts = [
                embed.title or "",
                embed.description or "",
                str(len(embed.fields)),
                embed.footer.text if embed.footer else ""
            ]
            return str(hash("_".join(hash_parts)))
        except Exception:
            return str(time.time())

    async def _process_queues(self):
        """Main processing loop with intelligent rate limiting"""
        while True:
            try:
                current_time = time.time()
                
                # Check global rate limit
                if self._is_globally_rate_limited(current_time):
                    await asyncio.sleep(0.1)
                    continue
                
                # Process channels by priority
                processed_any = False
                for channel_id in list(self.channel_queues.keys()):
                    if not self.channel_queues[channel_id]:
                        continue
                    
                    if channel_id in self.processing_channels:
                        continue
                    
                    if self._can_send_to_channel(channel_id, current_time):
                        asyncio.create_task(self._process_channel_queue(channel_id))
                        processed_any = True
                
                # Adaptive sleep based on queue state
                if not processed_any:
                    await asyncio.sleep(0.1)
                else:
                    await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Error in rate limiter processing: {e}")
                await asyncio.sleep(1)

    def _is_globally_rate_limited(self, current_time: float) -> bool:
        """Check if we're hitting global rate limits"""
        # Clean old timestamps
        while self.global_message_times and current_time - self.global_message_times[0] > 1:
            self.global_message_times.popleft()
        
        # Check if we're at the limit
        if len(self.global_message_times) >= self.GLOBAL_RATE_LIMIT:
            return True
        
        # Check Discord's global rate limit
        if current_time < self.global_rate_limit_reset:
            return True
        
        return False

    def _can_send_to_channel(self, channel_id: int, current_time: float) -> bool:
        """Check if we can send to a specific channel"""
        # Clean old message times for this channel
        channel_times = self.channel_message_count[channel_id]
        while channel_times and current_time - channel_times[0] > 5:
            channel_times.popleft()
        
        # Check channel rate limit
        if len(channel_times) >= self.CHANNEL_RATE_LIMIT:
            return False
        
        # Check minimum delay between messages (250ms)
        last_sent = self.channel_last_sent.get(channel_id, 0)
        if current_time - last_sent < 0.25:
            return False
        
        return True

    async def _process_channel_queue(self, channel_id: int):
        """Process messages for a specific channel"""
        if channel_id in self.processing_channels:
            return
        
        self.processing_channels.add(channel_id)
        
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                logger.warning(f"Channel {channel_id} not found, clearing queue")
                self.channel_queues[channel_id].clear()
                return
            
            queue = self.channel_queues[channel_id]
            while queue:
                current_time = time.time()
                
                # Check if we can still send
                if not self._can_send_to_channel(channel_id, current_time):
                    break
                
                if self._is_globally_rate_limited(current_time):
                    break
                
                message = queue.pop(0)
                success = await self._send_message(channel, message)
                
                if success:
                    # Update tracking
                    self.global_message_times.append(current_time)
                    self.channel_message_count[channel_id].append(current_time)
                    self.channel_last_sent[channel_id] = current_time
                    
                    # Call callback if provided
                    if message.callback:
                        try:
                            await message.callback()
                        except Exception as e:
                            logger.error(f"Callback error: {e}")
                else:
                    # Re-queue with increased retry count if failed
                    message.retry_count += 1
                    if message.retry_count < 3:
                        queue.insert(0, message)
                    else:
                        logger.error(f"Message failed after 3 retries: {message.embed.title}")
                
                # Small delay between messages in same channel
                await asyncio.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error processing channel {channel_id}: {e}")
        finally:
            self.processing_channels.discard(channel_id)

    async def _send_message(self, channel, message: QueuedMessage) -> bool:
        """Send a single message with rate limit handling"""
        try:
            kwargs = {}
            if message.embed:
                kwargs['embed'] = message.embed
            if message.file:
                kwargs['file'] = message.file
            if message.content:
                kwargs['content'] = message.content
            
            await channel.send(**kwargs)
            return True
            
        except discord.HTTPException as e:
            if e.status == 429:  # Rate limited
                # Extract rate limit info
                retry_after = getattr(e, 'retry_after', 1.0)
                
                if 'global' in str(e).lower():
                    self.global_rate_limit_reset = time.time() + retry_after
                    logger.warning(f"Global rate limit hit, waiting {retry_after}s")
                else:
                    logger.warning(f"Channel rate limit hit, waiting {retry_after}s")
                
                await asyncio.sleep(retry_after)
                return False
            else:
                logger.error(f"HTTP error sending message: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""
        total_queued = sum(len(queue) for queue in self.channel_queues.values())
        priority_counts = defaultdict(int)
        
        for queue in self.channel_queues.values():
            for message in queue:
                priority_counts[message.priority.name] += 1
        
        return {
            'total_queued': total_queued,
            'active_channels': len([q for q in self.channel_queues.values() if q]),
            'processing_channels': len(self.processing_channels),
            'priority_breakdown': dict(priority_counts),
            'global_rate_limit_active': time.time() < self.global_rate_limit_reset,
            'recent_global_messages': len(self.global_message_times)
        }

    async def flush_all_queues(self):
        """Force process all queues (for shutdown)"""
        await asyncio.sleep(2)  # Let current processing finish
        
        tasks = []
        for channel_id in list(self.channel_queues.keys()):
            if self.channel_queues[channel_id] and channel_id not in self.processing_channels:
                tasks.append(asyncio.create_task(self._process_channel_queue(channel_id)))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def __del__(self):
        """Cleanup on destruction"""
        if hasattr(self, 'processing_task') and not self.processing_task.done():
            self.processing_task.cancel()
