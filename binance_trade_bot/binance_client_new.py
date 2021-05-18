# -*- coding: utf-8 -*-
import glob
import os
import pathlib
import pickle
import time

from binance.client import Client

from binance_trade_bot.config import Config
from binance_trade_bot.logger import Logger


class BinanceClientNew(Client):
    def __init__(self, api_key, api_secret, config: Config, logger: Logger):
        try:
            super().__init__(api_key, api_secret, tld=config.BINANCE_TLD)
            self.is_client_ok = True
        except Exception as e:    # pylint: disable=broad-except
            self.is_client_ok = False

        self.config = config
        self.logger = logger
        self.socket_manager = None
        self.multiplex_socket_conn_key = None
        self.user_socket_conn_key = None
        self.all_ticker_dict = {}

    def start_multiplex_socket(self):
        streams_names = ["!ticker@arr"]
        conn_key = self.socket_manager.start_multiplex_socket(streams_names, self.process_multiplex_msg)
        self.multiplex_socket_conn_key = conn_key
        return conn_key

    def stop_multiplex_socket(self):
        if self.multiplex_socket_conn_key is not None:
            self.socket_manager.stop_socket(self.multiplex_socket_conn_key)

    def start_user_socket(self):
        conn_key = self.socket_manager.start_user_socket(self.process_user_msg)
        self.user_socket_conn_key = conn_key
        return conn_key

    def stop_user_socket(self):
        if self.user_socket_conn_key is not None:
            self.socket_manager.stop_socket(self.user_socket_conn_key)

    def _save_data(self, file_dir, data, timpstamp=None, prefix=None):
        if timpstamp is None:
            current_file_time = int(round(time.time() * 1000))
        else:
            current_file_time = int(timpstamp)

        data_file_name = "%015d" % current_file_time
        if prefix is not None:
            data_file_name = prefix + '_' + data_file_name
        data_file = pathlib.Path(file_dir) / data_file_name
        succ_file_name = data_file_name + ".succ"
        succ_file = pathlib.Path(file_dir) / succ_file_name

        df = open(data_file, "wb")
        df.write(pickle.dumps(data))
        df.close()
        os.renames(data_file, succ_file)

        return current_file_time

    def _get_all_file_names(self, file_dir, prefix):
        if prefix is None:
            all_file_names = glob.glob(os.path.join(file_dir, "*.succ"))
        else:
            all_file_names = glob.glob(os.path.join(file_dir, prefix + '_' + "*.succ"))
        return all_file_names

    def _get_file_time(self, file_name):
        file_name_prefix = file_name[-5-15:-5]
        return int(file_name_prefix)

    def delete_old_data(self, file_dir, current_file_time=None, prefix=None, file_cnt=1):
        all_file_names = self._get_all_file_names(file_dir, prefix)
        latest_file_names = self._get_latest_file_names(file_dir, prefix, file_cnt)

        for file_name_with_dir in all_file_names:
            file_name_tp = os.path.split(file_name_with_dir)
            file_name = file_name_tp[-1]
            file_time = self._get_file_time(file_name)

            delete_flag = True
            if len(latest_file_names) > 0:
                for latest_file_name in latest_file_names:
                    if file_name_with_dir == latest_file_name:
                        delete_flag = False
                        break
            # delete file 10 seconds before current_file_time
            if current_file_time is not None and file_time > current_file_time - 10 * 1000:
                delete_flag = False
            if not delete_flag:
                continue

            try:
                os.remove(file_name_with_dir)
            except Exception as e:
                self.logger.info(f"Exception occurred: {e}")

    def _delete_all_data(self, file_dir, prefix=None):
        all_file_names = self._get_all_file_names(file_dir, prefix)
        self.logger.info(f"delete all data files in {file_dir}")
        for file_name_with_dir in all_file_names:
            try:
                os.remove(file_name_with_dir)
            except Exception as e:
                self.logger.info(f"Exception occurred: {e}")

    def _process_ticker_msg(self, data_list):
        for tick_info in data_list:
            ticker_time = tick_info["E"]
            ticker_symbol = tick_info["s"]
            ticker_price = tick_info["c"]
            ticker = {"time": ticker_time, "symbol": ticker_symbol, "price": ticker_price}
            if ticker_symbol not in self.all_ticker_dict or \
               self.all_ticker_dict[ticker_symbol]["time"] < ticker_time:
                self.all_ticker_dict[ticker_symbol] = ticker

        file_dir = os.path.join(self.config.RAMDISK_DIR, 'ticker')
        data = list(self.all_ticker_dict.values())
        current_file_time = self._save_data(file_dir, data)

    def _process_other_msg(self, data_dict):
        pass

    def _process_order_msg(self, data_dict):
        order_status = {'symbol': data_dict['s'],
                        'orderId': data_dict['i'],
                        'time': data_dict['E'],
                        'side': data_dict['S'],
                        'price': data_dict['p'],
                        'status': data_dict['X'],
                        'execType': data_dict['x'],
                        'cummulativeQuoteQty': data_dict['Q']
                        }

        file_dir = os.path.join(self.config.RAMDISK_DIR, 'order')
        prefix = f"{order_status['symbol']}_{order_status['orderId']}"
        timestamp = order_status['time']
        data = order_status
        current_file_time = self._save_data(file_dir, data, timestamp, prefix)
        self.logger.info(f"order message received: {order_status}")

    def _save_account_msg(self, account_info):
        file_dir = os.path.join(self.config.RAMDISK_DIR, 'account')
        data = account_info
        if 'time' in account_info:
            timestamp = account_info['time']
            current_file_time = self._save_data(file_dir, data, timestamp)
        else:
            current_file_time = self._save_data(file_dir, data)

    def _process_account_msg(self, data_dict):
        account_info = {'time': data_dict['E'],
                        'balances': []
                        }
        balance_list = data_dict['B']
        for item in balance_list:
            balance_info = {'asset': item['a'],
                            'free': item['f'],
                            'locked': item['l']
                            }
            account_info['balances'].append(balance_info)

        self._save_account_msg(account_info)

    def process_multiplex_msg(self, msg):
        if "stream" not in msg:
            if "e" in msg and msg['e'] == 'error':
                self.logger.error(f"multiplex_socket error message received: {msg}")
                if msg['m'] == 'Max reconnect retries reached':
                    file_dir = os.path.join(self.config.RAMDISK_DIR, 'ticker')
                    self._delete_all_data(file_dir)
                    self.stop_multiplex_socket()
                    self.start_multiplex_socket()
            return

        data = msg["data"]
        # ticker message
        if isinstance(data, list):
            try:
                self._process_ticker_msg(msg['data'])
            except Exception as e:  # pylint: disable=broad-except
                self.logger.info(f"Unexpected Error in _process_ticker_msg {type(e)}, data is {msg['data']}")
        # other messages
        elif isinstance(msg['data'], dict):
            self._process_other_msg(msg['data'])
        else:
            self.logger.info(f"multiplex_socket unexpected message received: {msg}")

    def process_user_msg(self, msg):
        # self.logger.info(f"user_socket message received: {msg['e']}")
        if "e" in msg and msg['e'] == 'error':
            self.logger.error(f"user_socket error message received: {msg}")
            if msg['m'] == 'Max reconnect retries reached':
                file_dir = os.path.join(self.config.RAMDISK_DIR, 'order')
                self._delete_all_data(file_dir)
                file_dir = os.path.join(self.config.RAMDISK_DIR, 'account')
                self._delete_all_data(file_dir)
                self.stop_user_socket()
                self.start_user_socket()

        if msg['e'] == 'error':
            self.logger.error(f"user_socket error message received: {msg}")
            return

        # order message
        if msg['e'] == 'executionReport':
            try:
                self._process_order_msg(msg)
            except Exception as e:  # pylint: disable=broad-except
                self.logger.info(f"Unexpected Error in _process_order_msg {type(e)}, data is {msg}")
        # account message
        elif msg['e'] == 'outboundAccountPosition':
            try:
                self._process_account_msg(msg)
            except Exception as e:  # pylint: disable=broad-except
                self.logger.info(f"Unexpected Error in _process_account_msg {type(e)}, data is {msg}")
        else:
            self.logger.info(f"user_socket unexpected message received: {msg}")

    def _get_latest_file_names(self, file_dir, prefix=None, file_cnt=1):
        max_file_time = 0
        max_file_name = ""

        file_name_list = []
        retry_cnt = 0
        while max_file_name == "" or not os.path.exists(max_file_name):
            all_file_names = self._get_all_file_names(file_dir, prefix)

            for file_name_with_dir in all_file_names:
                file_name_tp = os.path.split(file_name_with_dir)
                file_name = file_name_tp[-1]
                file_time = self._get_file_time(file_name)

                if file_time > max_file_time:
                    max_file_name = file_name_with_dir
                    max_file_time = file_time
                    if len(file_name_list) < file_cnt:
                        file_name_list.append(max_file_name)
                    else:
                        for index in range(file_cnt-1):
                            file_name_list[index] = file_name_list[index+1]
                        file_name_list[file_cnt-1] = max_file_name

            retry_cnt += 1
            if retry_cnt == 5:
                # self.logger.error(f"data files in {file_dir} prefix={prefix} not found, revert to invoke parent class's function...")
                return []

        return file_name_list

    def _get_latest_file_name(self, file_dir, prefix=None):
        latest_file_names = self._get_latest_file_names(file_dir, prefix)
        if len(latest_file_names) == 1:
            return latest_file_names[0]

        return None

    def _load_data(self, latest_file_name):
        file_content = pathlib.Path(latest_file_name).read_bytes()
        result = pickle.loads(file_content)
        return result

    # override parent class's get_all_tickers() function
    def get_all_tickers(self):
        file_dir = os.path.join(self.config.RAMDISK_DIR, 'ticker')
        latest_file_name = self._get_latest_file_name(file_dir)
        if latest_file_name is None:
            result = super().get_all_tickers()
            return result

        return self._load_data(latest_file_name)

    # override parent class's get_symbol_ticker() function
    def get_symbol_ticker(self, **params):
        file_dir = os.path.join(self.config.RAMDISK_DIR, 'ticker')
        latest_file_name = self._get_latest_file_name(file_dir)
        if latest_file_name is None:
            result = super().get_symbol_ticker(**params)
            return result

        all_ticker_list = self._load_data(latest_file_name)
        if 'symbol' in params:
            ticker_symbol = params['symbol']
            if ticker_symbol is not None and ticker_symbol != '':
                for ticker in all_ticker_list:
                    if ticker["symbol"] == ticker_symbol:
                        return ticker
        else:
            return all_ticker_list

        return None

    # override parent class's get_order() function
    def get_order(self, **params):
        file_dir = os.path.join(self.config.RAMDISK_DIR, 'order')
        prefix = f"{params['symbol']}_{params['orderId']}"
        latest_file_name = self._get_latest_file_name(file_dir, prefix)
        if latest_file_name is None:
            self.logger.error(
                f"Fail load order from websocket data: {prefix}, call binance restful api.")
            result = super().get_order(**params)
            return result

        return self._load_data(latest_file_name)

    # override parent class's get_account() function
    def get_account(self, **params):
        file_dir = os.path.join(self.config.RAMDISK_DIR, 'account')
        latest_file_name = self._get_latest_file_name(file_dir)
        if latest_file_name is None:
            account_info = super().get_account(**params)
            if account_info is not None:
                self._save_account_msg(account_info)
            return account_info

        return self._load_data(latest_file_name)
