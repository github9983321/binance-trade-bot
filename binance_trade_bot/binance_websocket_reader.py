import os
import signal
import time

from .binance_api_manager_new import BinanceAPIManagerNew
from .config_new import ConfigNew
from .database import Database
from .file_tasks import prune_ramdisk_dir
from .logger import Logger
from .scheduler import SafeScheduler


def main():
    logger = Logger()
    config = ConfigNew()

    ramdisk_dir = config.RAMDISK_DIR
    if not os.path.exists(ramdisk_dir):
        logger.info(f"ramdisk_dir not exist, now create it")
        os.makedirs(ramdisk_dir)
        os.makedirs(os.path.join(ramdisk_dir, 'ticker'))
        os.makedirs(os.path.join(ramdisk_dir, 'order'))
        os.makedirs(os.path.join(ramdisk_dir, 'account'))

    schedule = SafeScheduler(logger)
    schedule.every(1).minutes.do(prune_ramdisk_dir).tag("pruning ramdisk dir")

    db = Database(logger, config)
    manager = BinanceAPIManagerNew(config, db, logger)

    try:
        conn_key = manager.start_multiplex_socket()
        logger.info(f"multiplex_socket started, conn_key='{conn_key}'")
        conn_key = manager.start_user_socket()
        logger.info(f"user_socket started, conn_key='{conn_key}'")
        manager.start_sock_manager()
        while True:
            schedule.run_pending()
            time.sleep(1)
    except SystemExit as se:
        logger.info(f"SystemExit occurred: {se}")
    except Exception as e:  # pylint: disable=broad-except
        logger.info(f"Exception occurred: {e}")
    finally:
        manager.stop_sock_manager()
        os._exit(0)


if __name__ == "__main__":
    main()
