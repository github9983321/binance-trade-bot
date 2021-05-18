import os
import signal
import time

from .binance_api_manager_new import BinanceAPIManagerNew
from .config_new import ConfigNew
from .database import Database
from .logger import Logger
from .scheduler import SafeScheduler
from .strategies import get_strategy


def main():
    logger = Logger()
    logger.info("Starting")

    config = ConfigNew()
    db = Database(logger, config)
    manager = BinanceAPIManagerNew(config, db, logger)
    strategy = get_strategy(config.STRATEGY)
    if strategy is None:
        logger.error("Invalid strategy name")
        return
    trader = strategy(manager, db, logger, config)
    logger.info(f"Chosen strategy: {config.STRATEGY}")

    logger.info("Creating database schema if it doesn't already exist")
    db.create_database()

    db.set_coins(config.SUPPORTED_COIN_LIST)
    db.migrate_old_state()

    trader.initialize()

    schedule = SafeScheduler(logger)
    schedule.every(config.SCOUT_SLEEP_TIME).seconds.do(trader.scout).tag("scouting")
    schedule.every(1).minutes.do(trader.update_values).tag("updating value history")
    schedule.every(1).minutes.do(db.prune_scout_history).tag("pruning scout history")
    schedule.every(1).hours.do(db.prune_value_history).tag("pruning value history")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except SystemExit as se:
        logger.info(f"SystemExit occurred: {se}")
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"Exception occurred: {e}")
    finally:
        os._exit(0)
