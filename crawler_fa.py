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
from scripts import buyerFullahead
from scripts import supabaseUtil

# Fullaheadクローラー
class fullaheadCrawler():
    def exec(self,wrapper,supabase):
        # 市場情報保存用のディレクトリを用意する
        market_dir = './data/market/fullahead'
        Path(market_dir).mkdir(parents=True, exist_ok=True)
        card_dir = './data/card'
        Path(card_dir).mkdir(parents=True, exist_ok=True)

        self._crawl(wrapper, market_dir)
        self._upload(supabase, card_dir, market_dir)

    # リストを生成
    def _getPageList(self):
        listItem = [
            {'name': '拡張パック【SVシリーズ】', 'num': 0},
            {'name': '強化拡張パック【SVシリーズ】', 'num': 1},
            {'name': 'コンセプトパック【SVシリーズ】', 'num': 2},
            {'name': '構築デッキ【SVシリーズ】', 'num': 3},
            {'name': 'プロモーションカード【SVシリーズ】', 'num': 4},
            {'name': 'その他デッキ・BOX【SVシリーズ】', 'num': 5},
            {'name': '拡張パック【Sシリーズ】', 'num': 7},
            {'name': '強化拡張パック【Sシリーズ】', 'num': 8},
            {'name': 'コンセプトパック【Sシリーズ】', 'num': 9},
            {'name': '構築デッキ【Sシリーズ】', 'num': 10},
            {'name': 'プロモーションカード【Sシリーズ】', 'num': 11},
            {'name': 'その他デッキ・BOX【Sシリーズ】', 'num': 12},
        ]
        return listItem
    
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
        fullaheadBot = buyerFullahead.buyerFullaheadBot()
        listItem = self._getPageList()
        
        for item in listItem:
            print(item['name'])
            fullaheadBot.download(wrapper,item['num'],market_dir)

     # アップロードする
    def _upload(self, supabase, card_dir:str, market_dir:str):
        writer = supabaseUtil.batchWriter()
        editor = supabaseUtil.batchEditor()
        cardDf = self._getCardDf(card_dir)

        loader = buyerFullahead.fullaheadBuyCsvLoader()
        df = loader.load(market_dir)
        records = df.to_dict(orient='records')

        for i in range(0, len(records), 100):
            # 100件ずつPOSTする
            batch = records[i: i+100]
            converted = editor.getShopBuyupDaily4Fullahead(batch, cardDf)
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

fullahead = fullaheadCrawler()
fullahead.exec(wrapper, supabase)

wrapper.end()
