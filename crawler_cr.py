import os
from get_chrome_driver import GetChromeDriver
from selenium import webdriver
import socket
import pandas as pd
import glob
from pathlib import Path
from supabase import create_client, Client 
from scripts import seleniumDriverWrapper as wrap
from scripts import buyerCardrush
from scripts import supabaseUtil

# Cardrushクローラー
class CardrushCrawler():
    def exec(self,wrapper,supabase):
        # 市場情報保存用のディレクトリを用意する
        market_dir = './data/market/cardrush'
        Path(market_dir).mkdir(parents=True, exist_ok=True)
        card_dir = './data/card'
        Path(card_dir).mkdir(parents=True, exist_ok=True)

        self._crawl(wrapper, market_dir)
        self._upload(supabase, card_dir, market_dir)
    
    # 全カードリストを取得
    def _getCardDf(self, card_dir:str):
        card_list = []
        files = glob.glob(card_dir+'/*.csv')
        for file in files:
            readDf = pd.read_csv(
                file,
                encoding="utf_8_sig", sep=",",
                header=0)
            card_list.append(readDf)
        cardDf = pd.concat(card_list, axis=0, ignore_index=True, sort=True)
        return cardDf
    
    # クロールする
    def _crawl(self, wrapper, market_dir:str):
        cardrushBot = buyerCardrush.buyerCardrushBot()
        cardrushBot.download(wrapper,market_dir)

     # アップロードする
    def _upload(self, supabase, card_dir:str, market_dir:str):
        writer = supabaseUtil.batchWriter()
        editor = supabaseUtil.batchEditor()
        cardDf = self._getCardDf(card_dir)

        loader = buyerCardrush.cardrushBuyCsvLoader()
        df = loader.load(market_dir)
        records = df.to_dict(orient='records')

        for i in range(0, len(records), 100):
            # 100件ずつPOSTする
            batch = records[i: i+100]
            converted = editor.getShopBuyupDaily4Cardrush(batch, cardDf)
            print('Write log no.:'+str(i))
            writer.write(supabase, "shop_buyup_daily", converted)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_ANON_KEY")
service_key: str = os.environ.get("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(url, key)
supabase.postgrest.auth(service_key)

get_driver = GetChromeDriver()
get_driver.install()

wrapper = wrap.seleniumDriverWrapper()
wrapper.begin(webdriver)
cardrushBot = buyerCardrush.buyerCardrushBot()

ip = socket.gethostbyname(socket.gethostname())
print(ip)

cardrush = CardrushCrawler()
cardrush.exec(wrapper, supabase)

wrapper.end()
