import os

from .binance_api_manager_new import BinanceAPIManagerNew
from .config_new import ConfigNew
from .database import Database
from .logger import Logger


def prune_ramdisk_dir():
    logger = Logger()
    config = ConfigNew()
    db = Database(logger, config)

    ramdisk_dir = config.RAMDISK_DIR
    ticker_dir = os.path.join(ramdisk_dir, 'ticker')
    order_dir = os.path.join(ramdisk_dir, 'order')
    account_dir = os.path.join(ramdisk_dir, 'account')

    manager = BinanceAPIManagerNew(config, db, logger)

    manager.delete_old_data(ticker_dir, file_cnt=3)
    manager.delete_old_data(order_dir, file_cnt=3)
    manager.delete_old_data(account_dir, file_cnt=3)
