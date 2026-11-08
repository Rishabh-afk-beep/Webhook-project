"""Bootstrap script — creates a demo tenant and API key.

Usage:
    python -m scripts.create_demo_tenant

This script is for development/testing only. The raw API key is printed
once to stdout and is NEVER stored in plaintext.
"""
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import get_settings
from app.core.security import generate_api_key
from app.db.session import create_session
from app.models.tenant import Tenant
from app.models.api_key import ApiKey


def create_demo_tenant() -> None:
    """Create a demo tenant and API key, printing the key to stdout."""
    settings = get_settings()
    session = create_session()

    try:
        # Check if demo tenant already exists
        from sqlalchemy import select
        existing = session.execute(
            select(Tenant).where(Tenant.name == settings.default_tenant_name)
        ).scalar_one_or_none()

        if existing:
            print(f"Demo tenant '{settings.default_tenant_name}' already exists (ID: {existing.id})")
            print("To create a new API key, delete the tenant first or use a different name.")
            session.close()
            return

        # Create tenant
        tenant = Tenant(name=settings.default_tenant_name)
        session.add(tenant)
        session.flush()

        # Generate API key
        raw_key, key_prefix, key_hash = generate_api_key()

        api_key = ApiKey(
            tenant_id=tenant.id,
            key_prefix=key_prefix,
            key_hash=key_hash,
        )
        session.add(api_key)
        session.commit()

        print("=" * 60)
        print("  Demo Tenant Created Successfully")
        print("=" * 60)
        print(f"  Tenant ID:   {tenant.id}")
        print(f"  Tenant Name: {tenant.name}")
        print(f"  API Key ID:  {api_key.id}")
        print(f"  Key Prefix:  {key_prefix}")
        print()
        print(f"  API Key:     {raw_key}")
        print()
        print("  ⚠  Save this API key now! It will NOT be shown again.")
        print("  ⚠  Do NOT commit this key to version control.")
        print("=" * 60)

    except Exception as e:
        session.rollback()
        print(f"Error creating demo tenant: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    create_demo_tenant()
