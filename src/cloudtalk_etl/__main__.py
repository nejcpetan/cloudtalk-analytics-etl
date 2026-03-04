"""Entry point: python -m cloudtalk_etl"""
from cloudtalk_etl.config import Settings
from cloudtalk_etl.logging import setup_logging
from cloudtalk_etl.main import run_etl


def main() -> None:
    settings = Settings()
    setup_logging(settings.log_level)
    run_etl()


if __name__ == "__main__":
    main()
