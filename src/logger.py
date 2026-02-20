import logging
import sys
import json
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_data"):
            log_data["data"] = record.extra_data
        return json.dumps(log_data)


class LoggerFactory:
    _loggers = {}
    _log_dir = Path(__file__).parent.parent / "logs"
    _initialized = False

    @classmethod
    def _ensure_log_dir(cls):
        if not cls._initialized:
            cls._log_dir.mkdir(exist_ok=True)
            cls._initialized = True

    @classmethod
    def get_logger(
        cls,
        name: str,
        level: int = logging.INFO,
        log_to_file: bool = True,
        log_to_console: bool = True,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
    ) -> logging.Logger:
        if name in cls._loggers:
            return cls._loggers[name]

        cls._ensure_log_dir()

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers.clear()
        logger.propagate = False

        text_formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if log_to_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(text_formatter)
            logger.addHandler(console_handler)

        if log_to_file:
            file_handler = RotatingFileHandler(
                filename=cls._log_dir / f"{name}.log",
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(text_formatter)
            logger.addHandler(file_handler)

            json_handler = RotatingFileHandler(
                filename=cls._log_dir / f"{name}.json.log",
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            json_handler.setFormatter(JsonFormatter())
            logger.addHandler(json_handler)

            error_handler = RotatingFileHandler(
                filename=cls._log_dir / "errors.log",
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(text_formatter)
            logger.addHandler(error_handler)

        cls._loggers[name] = logger
        return logger


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    return LoggerFactory.get_logger(name, level=level)
