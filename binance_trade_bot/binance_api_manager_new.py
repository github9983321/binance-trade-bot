import math
import time
import traceback

from twisted.internet import reactor

from binance.exceptions import BinanceAPIException
from binance.websockets import BinanceSocketManager
from cachetools import TTLCache, cached

from binance_trade_bot.binance_api.binance_client_new import BinanceClientNew
from binance_trade_bot.binance_api_manager import AllTickers, BinanceAPIManager
from binance_trade_bot.config_new import ConfigNew
from binance_trade_bot.database import Database
from binance_trade_bot.logger import Logger
from binance_trade_bot.models import Coin


class BinanceAPIManagerNew(BinanceAPIManager):
    def __init__(self, config: ConfigNew, db: Database, logger: Logger):
        self.db = db
        self.logger = logger
        self.config = config
        api_key = self.config.BINANCE_API_KEY
        api_secret = self.config.BINANCE_API_SECRET_KEY
        self.binance_client = BinanceClientNew(api_key, api_secret, config, logger)
        if self.binance_client.is_client_ok:
            self.socket_manager = BinanceSocketManager(self.binance_client)
            self.binance_client.socket_manager = self.socket_manager

    def start_multiplex_socket(self):
        conn_key = self.binance_client.start_multiplex_socket()
        return conn_key

    def start_user_socket(self):
        conn_key = self.binance_client.start_user_socket()
        return conn_key

    def start_sock_manager(self):
        self.socket_manager.start()
        self.logger.info("BinanceSocketManager started")

    def stop_socket(self):
        self.binance_client.stop_multiplex_socket()
        self.binance_client.stop_user_socket()

    def stop_sock_manager(self):
        self.stop_socket()
        self.socket_manager.close()
        reactor.stop()
        self.logger.info("BinanceSocketManager closed")

    def get_all_market_tickers(self) -> AllTickers:
        """
        Get ticker price of all coins
        """
        # return super().get_all_market_tickers()
        return AllTickers(self.binance_client.get_all_tickers())

    def get_all_balances(self):
        try:
            account_info = self.binance_client.get_account()
        except BinanceAPIException as e:
            self.logger.error(f"Error in get_all_balances(): {e}")
            if e.code == -1003:
                pass
            return None

        return account_info["balances"]

    def get_currency_balance(self, currency_symbol: str):
        """
        Get balance of a specific coin
        """
        try:
            account_info = self.binance_client.get_account()
        except BinanceAPIException as e:
            self.logger.error(f"Error in get_currency_balance(): {e}")
            if e.code == -1003:
                pass
            return None

        for currency_balance in account_info["balances"]:
            if currency_balance["asset"] == currency_symbol:
                return float(currency_balance["free"])
        return None

    def get_market_ticker_price(self, ticker_symbol: str):
        """
        Get ticker price of a specific coin
        """
        for ticker in self.binance_client.get_symbol_ticker():
            if ticker["symbol"] == ticker_symbol:
                return float(ticker["price"])
        return None

    def delete_old_data(self, file_dir, file_cnt=1):
        self.binance_client.delete_old_data(file_dir, file_cnt=file_cnt)
