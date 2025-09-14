import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

USER_AGENT = os.getenv(
    "USER_AGENT",
    "web2sheets-tracker/0.1 (+https://example.com; contact=none)"
)

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "0.7"))
