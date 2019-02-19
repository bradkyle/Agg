from binance.client import Client
from binance.websockets import BinanceSocketManager
import threading
import time
import copy
from google.cloud import pubsub_v1
import os
import json

class WSBinance():
    def __init__(
        self,
        quote_asset,
        levels=20,
        interval="1m",
        update_every=10,
        project_id="axiom",
        pubsub_topic="features"
    ):

        self.ASKS = {};
        self.BIDS  = {};

        self.client = Client("", "")
        self.bm = BinanceSocketManager(self.client)

        self.quote_asset = quote_asset
        self.depth_ws_suffix = "@depth"+str(levels)
        self.kline_ws_suffix = "@kline_"+str(interval)

        self.interval = interval
        self.update_every = update_every

        self.set_conf()

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=project_id,
            topic=pubsub_topic
        )

        thread = threading.Thread(target=self.update_conf, args=())
        thread.daemon = True                            # Daemonize thread
        if not thread.is_alive():
            thread.start()

    # TODO add try catch
    def set_conf(self):
        try:
            info = self.client.get_exchange_info()
            linfo = [x['symbol'] for x in info['symbols'] if self.check_quote_asset(self.quote_asset,x['symbol'])]
            if len(linfo) > 5:
                self.kline_ws_list = [y.lower()+self.kline_ws_suffix for y in linfo]
                self.depth_ws_list = [y.lower()+self.depth_ws_suffix for y in linfo]
                return True
        except Exception as e:
            print(e)
            return False

    def update_conf(self):
        while True:
            time.sleep(self.update_every)
            prev_kline_ws_list = copy.copy(self.kline_ws_list)
            prev_depth_ws_list = copy.copy(self.depth_ws_list)
            self.set_conf()
            if set(prev_depth_ws_list) != set(self.depth_ws_list) or\
               set(prev_kline_ws_list) != set(self.kline_ws_list):
               self.reset()            

    def process_depth(self, msg):
        symbol = msg['stream'].replace(self.depth_ws_suffix, '').upper()
        bids, asks = self.get_depth(msg['data'])
        self.ASKS[symbol] = asks
        self.BIDS[symbol] = bids
    
    def derive_base_asset(self, quote_asset, symbol):
        return symbol.replace(quote_asset, '')

    def check_quote_asset(self, quote_asset, symbol):
        return symbol.endswith(quote_asset);

    def get_asks(self, symbol):
        return self.ASKS[symbol]

    def get_bids(self, symbol):
        return self.BIDS[symbol]

    def process_kline(self, msg):
        d = msg['data']
        k = d['k']
        if d['s'] in self.ASKS:
            kline = {
                'Id': [k['s'], k['t']],
                'exchange': 'binance',
                'eventId': ['binance', 'feature', d['s'], d['E']],
                'eventTime': d['E'],                    
                'startTime': k['t'],
                'endTime': k['T'],
                'symbol': k['s'],
                'baseAsset': self.derive_base_asset(self.quote_asset,k['s']),
                'quoteAsset': self.quote_asset,
                'interval': k['i'],
                'open': float(k['o']),
                'close': float(k['c']),
                'high': float(k['h']),
                'low': float(k['l']),
                'volume': float(k['v']),
                'trades': k['n'],
                'quoteAssetVolume': float(k['q']),
                'takerBuyBaseAssetVolume': float(k['V']),
                'takerBuyQuoteAssetVolume': float(k['Q']),
                'isFinal': k['x'],
                'asks': self.get_asks(k['s']),
                'bids': self.get_bids(k['s']),
            };
            message = json.dumps(kline).encode('utf-8')
            self.publisher.publish(self.topic_name, message)

    def get_depth(self, data):
        bids = [{'price':float(l[0]), 'quantity':float(l[1])} for l in data['bids']]
        asks = [{'price':float(l[0]), 'quantity':float(l[1])} for l in data['asks']]
        return bids, asks

    def setup(self):
        self.depth_key = self.bm.start_multiplex_socket(
            self.depth_ws_list,
            self.process_depth
        )
        self.kline_key = self.bm.start_multiplex_socket(
            self.kline_ws_list,
            self.process_kline
        )

    def start(self):
        self.bm.start()

    def stop(self):
        print("Stopping...")
        self.bm.stop_socket(self.depth_key)
        self.bm.stop_socket(self.kline_key)

    def run(self):
        self.setup()
        self.start()

    def reset(self):
            self.stop()
            self.run()
    

if __name__ == '__main__':
    ws = WSBinance("BTC", project_id="axiom-227418") # TODO USDT, BNB, BTC, ETH
    ws.run()