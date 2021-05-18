import configparser
import os

from .config import Config

CFG_FL_NAME = "user.cfg"
USER_CFG_SECTION = "binance_user_config"
WEBSOCKET_CFG_SECTION = "binance_websocket_config"


class ConfigNew(Config):  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self):
        super().__init__()
        config = configparser.ConfigParser()
        config.read(CFG_FL_NAME)
        self.set_websocket_config(config)

    def set_websocket_config(self, config):
        self.RAMDISK_DIR = os.environ.get("RAMDISK_DIR") or config.get(WEBSOCKET_CFG_SECTION, "ramdisk_dir")
