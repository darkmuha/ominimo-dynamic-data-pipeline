import logging
from datetime import datetime
from pathlib import Path


def setup_logging(
    log_dir: str = "Data/output/logs", log_level: int = logging.INFO
) -> logging.Logger:
    """
    Sets up file-based logging with timestamped log files.

    :param log_dir: Directory to save log files
    :param log_level: Logging level (default: INFO)
    :return: Configured logger instance
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"pipeline_{timestamp}.log"

    logger = logging.getLogger("motor_ingestion")
    logger.setLevel(log_level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized. Log file: {log_file}")

    return logger


def get_logger() -> logging.Logger:
    """
    Gets the configured logger instance.

    :return: Logger instance
    """
    logger = logging.getLogger("motor_ingestion")
    if not logger.handlers:
        return setup_logging()
    return logger
