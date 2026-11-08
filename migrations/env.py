"""Alembic environment configuration.

Loads DATABASE_URL from the application settings and configures
Alembic to use the application's SQLAlchemy models.
"""
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

# Import all models so Alembic can detect them
from app.db.base import Base
from app.models.tenant import Tenant  # noqa: F401
from app.models.api_key import ApiKey  # noqa: F401
from app.models.destination import Destination  # noqa: F401
from app.models.event import Event  # noqa: F401
from app.models.delivery import EventDelivery  # noqa: F401
from app.models.delivery_attempt import DeliveryAttempt  # noqa: F401
from app.models.outbox import OutboxEntry  # noqa: F401
from app.core.config import get_settings

# Alembic Config object
config = context.config

# Set the database URL from application settings
settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database_url)

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# MetaData target for autogenerate support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — emit SQL to stdout."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
