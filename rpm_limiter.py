import time, random, re
from langchain_google_genai._common import GoogleGenerativeAIError

class RPMLimiter:
    """Limits to max_rpm calls per minute (sequentially)"""
    def __init__(self, max_rpm: int):
        self.min_interval = 60.0 / max_rpm
        self._last = 0.0

    def wait(self):
        now = time.monotonic()
        sleep_for = self.min_interval - (now - self._last)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last = time.monotonic()

def _extract_retry_seconds(msg: str) -> float | None:
    m = re.search(r"retry in ([0-9.]+)s", msg, re.IGNORECASE)
    if m:
        return float(m.group(1))
    m = re.search(r"retryDelay'\s*:\s*'(\d+)s'", msg)
    if m:
        return float(m.group(1))
    return None

def call_with_retry(fn, *args, limiter=None, max_retries=8, base_delay=2.0, max_delay=60.0, **kwargs):
    """
    - limiter.wait() before each request (throttling)
    - on 429/RESOURCE_EXHAUSTED: wait and retry
    """
    for attempt in range(max_retries):
        try:
            if limiter:
                limiter.wait()
            return fn(*args, **kwargs)
        except GoogleGenerativeAIError as e:
            msg = str(e)
            if "429" in msg:
                retry_s = _extract_retry_seconds(msg)
                
                if retry_s is None:
                    retry_s = min(max_delay, base_delay * (2 ** attempt))
                retry_s += random.uniform(0.2, 1.0)
                time.sleep(retry_s)
                continue
            raise  
    raise RuntimeError("Too many retries (rate limit).")