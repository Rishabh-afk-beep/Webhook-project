"""Security utilities: API key hashing, HMAC signing, URL validation."""
import hashlib
import hmac
import ipaddress
import secrets
import time
from urllib.parse import urlparse

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# --- API Key Management ---

API_KEY_PREFIX_LENGTH = 8
API_KEY_BYTE_LENGTH = 32


def generate_api_key() -> tuple[str, str, str]:
    """Generate a new API key.

    Returns:
        Tuple of (raw_key, key_prefix, key_hash).
        The raw_key must be shown to the user exactly once and never stored.
    """
    raw_key = secrets.token_urlsafe(API_KEY_BYTE_LENGTH)
    key_prefix = raw_key[:API_KEY_PREFIX_LENGTH]
    key_hash = hash_api_key(raw_key)
    return raw_key, key_prefix, key_hash


def hash_api_key(raw_key: str) -> str:
    """Hash an API key using SHA-256.

    We use SHA-256 rather than bcrypt because API keys are high-entropy
    random tokens (not user-chosen passwords), making brute-force
    infeasible. SHA-256 allows O(1) lookup by prefix + hash comparison
    without the per-verification cost of bcrypt.
    """
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    """Verify an API key against its stored hash using constant-time comparison."""
    computed_hash = hash_api_key(raw_key)
    return hmac.compare_digest(computed_hash, stored_hash)


# --- HMAC Webhook Signing ---

def compute_webhook_signature(payload_bytes: bytes, signing_secret: str, timestamp: int) -> str:
    """Compute HMAC-SHA256 signature for a webhook payload.

    Signature input: "{timestamp}.{payload_bytes_utf8}"
    This binds the timestamp to the payload to prevent replay attacks.

    Args:
        payload_bytes: The exact serialized JSON body being sent.
        signing_secret: The destination's signing secret.
        timestamp: Unix timestamp included in the request.

    Returns:
        Hex-encoded HMAC-SHA256 signature string.
    """
    message = f"{timestamp}.".encode("utf-8") + payload_bytes
    signature = hmac.new(
        signing_secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()
    return signature


def generate_signing_secret() -> str:
    """Generate a cryptographically secure signing secret for a destination."""
    return secrets.token_hex(32)


# --- URL Validation ---

# Private/reserved IP ranges that must be blocked
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),       # Loopback
    ipaddress.ip_network("10.0.0.0/8"),         # Private
    ipaddress.ip_network("172.16.0.0/12"),      # Private
    ipaddress.ip_network("192.168.0.0/16"),     # Private
    ipaddress.ip_network("169.254.0.0/16"),     # Link-local
    ipaddress.ip_network("::1/128"),            # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),           # IPv6 private
    ipaddress.ip_network("fe80::/10"),          # IPv6 link-local
]

# Cloud metadata endpoints
_BLOCKED_HOSTNAMES = {
    "metadata.google.internal",
    "metadata.goog",
    "169.254.169.254",
}


def validate_destination_url(url: str) -> tuple[bool, str]:
    """Validate a destination URL for safety.

    Checks:
    1. URL is well-formed with http or https scheme.
    2. Hostname is not a blocked private/loopback/metadata address.
    3. If hostname is an IP, it's not in a private range.
    4. Allowed test hostnames bypass private checks (dev only).

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        parsed = urlparse(url)
    except Exception:
        return False, "Malformed URL"

    # Check scheme
    if parsed.scheme not in ("http", "https"):
        return False, f"Unsupported URL scheme '{parsed.scheme}'. Only http and https are allowed."

    hostname = parsed.hostname
    if not hostname:
        return False, "URL must include a hostname."

    # Check against blocked hostnames
    if hostname.lower() in _BLOCKED_HOSTNAMES:
        return False, f"Hostname '{hostname}' is blocked (cloud metadata endpoint)."

    # Check if hostname is in the allowed test hosts list
    settings = get_settings()
    if hostname in settings.allowed_test_hosts:
        return True, ""

    # Try to parse as IP address
    try:
        ip = ipaddress.ip_address(hostname)
        for network in _BLOCKED_NETWORKS:
            if ip in network:
                return False, f"IP address '{hostname}' is in a blocked private/reserved range."
        return True, ""
    except ValueError:
        pass  # Not an IP address, it's a hostname

    # For non-IP hostnames, check common patterns
    if hostname in ("localhost", "localhost.localdomain"):
        return False, f"Hostname '{hostname}' resolves to a loopback address."

    # Hostname is a domain name - basic validation
    if "." not in hostname and hostname not in settings.allowed_test_hosts:
        return False, f"Hostname '{hostname}' does not appear to be a valid domain. Add to ALLOW_LOCAL_TEST_DESTINATIONS if this is a test host."

    return True, ""
