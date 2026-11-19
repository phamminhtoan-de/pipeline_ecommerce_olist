from functools import lru_cache
from sqlalchemy import create_engine, text
from src.config.settings import SRC_SQLALCHEMY_URI, TGT_SQLALCHEMY_URI
from src.core.logger import get_logger

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_source_engine():
    return create_engine(
        SRC_SQLALCHEMY_URI,
        pool_pre_ping=True,
    )


@lru_cache(maxsize=1)
def get_target_engine():
    return create_engine(
        TGT_SQLALCHEMY_URI,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


def test_source_connection():
    try:
        with get_source_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Kết nối Source Database thành công!")
        return True
    except Exception as exc:
        logger.error(f"Lỗi kết nối Source Database (SQL Server): {exc}")
        raise exc


def test_target_connection():
    try:
        with get_target_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Kết nối Target Database thành công!")
        return True
    except Exception as exc:
        logger.error(f"Lỗi kết nối Target Database (PostgreSQL): {exc}")
        raise exc


def test_connections():
    test_source_connection()
    test_target_connection()
    return True
