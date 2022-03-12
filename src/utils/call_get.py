import requests
from ratelimit import RateLimitException, limits
from tenacity import *
from yarl import URL

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(max=60),
    stop=stop_after_attempt(3),
)
@retry(
    retry=retry_if_exception_type(RateLimitException),
    wait=wait_fixed(60),
    stop=stop_after_delay(360),
)
@limits(calls=50, period=60)
def call_get(url: URL, params: dict) -> str:
    """
    Get the call from API
    """
    return requests.get(url.update_query(params)).text
