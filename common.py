import asyncio
import functools
import logging
import random
import time
from typing import Callable, Optional
from pyrogram.errors import FloodWait
MAX_MSG_LEN = 4095

logger = logging.getLogger(__name__)

_global_flood_until: float = 0.0
_global_lock = asyncio.Lock()

def _now() -> float:
    return time.time()

def set_global_flood(wait_seconds: float) -> None:
    global _global_flood_until
    _global_flood_until = max(_global_flood_until, _now() + wait_seconds)

async def _await_global_if_needed():
    remaining = _global_flood_until - _now()
    if remaining > 0:
        logger.info("Global flood active, sleeping %.1f s", remaining)
        await asyncio.sleep(remaining)

def flood_retry(
    *,
    max_retries: int = 5,
    max_wait_for_single: int = 60 * 60,
    max_total_wait: int = 60 * 60 * 6,
    apply_globally: bool = False,
    safety: float = 1.05,
    jitter: float = 1.5,
    logger_obj: Optional[logging.Logger] = None
) -> Callable:
    log = logger_obj or logger

    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                attempts = 0
                total_waited = 0.0

                if apply_globally:
                    await _await_global_if_needed()

                while True:
                    try:
                        return await func(*args, **kwargs)
                    except FloodWait as e:
                        try:
                            wait = int(getattr(e, "value"))
                        except Exception:
                            wait = 1

                        attempts += 1
                        wait = max(0, wait)

                        if wait > max_wait_for_single:
                            log.exception(
                                "FloodWait required wait=%s exceeds max_wait_for_single=%s. Re-raising.",
                                wait, max_wait_for_single
                            )
                            raise

                        total_waited += wait
                        if total_waited > max_total_wait or attempts > max_retries:
                            log.exception(
                                "FloodRetry limits exceeded (attempts=%s, total_waited=%.1f). Re-raising.",
                                attempts, total_waited
                            )
                            raise

                        if apply_globally:
                            async with _global_lock:
                                set_global_flood(wait)

                        # safety + jitter
                        jitter_amount = random.random() * jitter
                        sleep_for = wait * safety + jitter_amount
                        log.warning(
                            "FloodWait caught: need to wait %ds (raw=%s). Attempt %d/%d. Sleeping %.2fs",
                            wait, getattr(e, "value", None), attempts, max_retries, sleep_for
                        )
                        await asyncio.sleep(sleep_for)
                        if apply_globally:
                            await _await_global_if_needed()
                    except Exception as other:
                        raise
            return wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(wrapper(*args, **kwargs))
            async def _async_impl(*a, **kw):
                return func(*a, **kw)
            return sync_wrapper
    return decorator

@flood_retry(max_retries=3, apply_globally=False)
async def safe_send(client, chat_id, text: str, **kwargs):
    await client.send_message(chat_id, text, parse_mode=None, **kwargs)

class TelegramAccumulator:
    def __init__(self, client, chat_id: int | str, max_len: int = MAX_MSG_LEN, send_delay: float = 0.0):
        self.client = client
        self.chat_id = chat_id
        self.max_len = max_len
        self.send_delay = send_delay
        self._buf = ""

    async def add(self, text: str, **kwargs):
        if not text:
            return
        self._buf += text
        await self._flush_while_needed(**kwargs)

    async def _flush_while_needed(self, **kwargs):
        while len(self._buf) >= self.max_len:
            part = self._take_part(self._buf)
            await safe_send(self.client, self.chat_id, part, **kwargs)
            # убрать отправленную часть + ведущие переводы строки/пробелы
            self._buf = self._buf[len(part):].lstrip("\n ").lstrip()
            if self.send_delay:
                await asyncio.sleep(self.send_delay)

    def _take_part(self, text: str) -> str:
        if len(text) <= self.max_len:
            return text
        cut = text.rfind("\n", 0, self.max_len)
        if cut != -1 and cut > 0:
            return text[:cut]
        cut = text.rfind(" ", 0, self.max_len)
        if cut != -1 and cut > 0:
            return text[:cut]
        return text[: self.max_len]

    async def flush(self, *, strip: bool = True, **kwargs):
        if not self._buf:
            return
        text = self._buf.strip() if strip else self._buf
        if text:
            await safe_send(self.client, self.chat_id, text, **kwargs)
        self._buf = ""