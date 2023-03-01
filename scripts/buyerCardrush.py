from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import csv
import os
import time
import datetime
import re
from . import jst
from . import seleniumDriverWrapper as wrap
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
import traceback

class cardrushListParser():
    def __init__(self, _html, _url):
        self.__html = _html
        self.__url = _url

    def getItemList(self):
        soup = BeautifulSoup(self.__html, 'html.parser')
        l = list()
        trList = soup.find_all("tr")
        for tr in trList:
            # tr の中に 6 個の td がある
            tds = tr.find_all("td")
            name = ''
            made = ''
            rarity = ''
            cn = ''
            cardtype = ''
            price = ''
            mirror = 0
            for index, td in enumerate(tds):
                if index == 0: name = td.get_text()
                elif index == 1: made = td.get_text()
                elif index == 2: rarity = td.get_text()
                elif index == 3: cn = td.get_text()
                elif index == 4: cardtype = td.get_text()
                elif index == 5: price = td.get_text()
            if len(name) == 0 | len(price) == 0 | len(cn) == 0:
                continue
            price = price.replace(',','')
            if price.isdigit() == False:
                continue
            if made.find('ミラー') != -1:
                mirror = 1
            l.append({
                "market": 'cardrush',
                "link": self.__url,
                "price": int(price),
                "name": name,
                "made": made,
                "rarity": rarity,
                "cn": cn,
                "type": cardtype,
                "mirror": mirror,
                "date": None,
                "datetime": None,
            })
        return l

class cardrushBuyCsv():
    def __init__(self,_out_dir):
        dt = jst.now().replace(microsecond=0)
        self.__out_dir = _out_dir
        self.__list = list()
        self.__date = str(dt.date())
        self.__datetime = str(dt)
        self.__file = _out_dir+'/'+self.__datetime.replace("-","_").replace(":","_").replace(" ","_")+'_cardrush.csv'

    def init(self):
        labels = [
            'market',
            'link',
            'price',
            'name', 
            'made', 
            'rarity', 
            'cn', 
            'type', 
            'mirror',
            'date',
            'datetime'
        ]
        try:
            with open(self.__file, 'w', newline="", encoding="utf_8_sig") as f:
                writer = csv.DictWriter(f, fieldnames=labels)
                writer.writeheader()
                f.close()
        except IOError:
            print("I/O error")

    def add(self, data):
        data['date'] = str(self.__date)
        data['datetime'] = str(self.__datetime)
        self.__list.append(data)
        
    def save(self):
        if len(self.__list) == 0:
            return
        df = pd.DataFrame.from_dict(self.__list)
        if os.path.isfile(self.__file) == False:
            self.init()
        df.to_csv(self.__file, index=False, encoding='utf_8_sig')

class cardrushBuyCsvLoader():
    def load(self, _data_dir):
        df = pd.DataFrame(index=[], columns=[
            'market',
            'link',
            'price',
            'name', 
            'made', 
            'rarity', 
            'cn', 
            'type', 
            'mirror',
            'date',
            'datetime'
        ])
        files = os.listdir(_data_dir)
        files_file = [f for f in files if os.path.isfile(os.path.join(_data_dir, f)) and '.csv' in f]
        for item in files_file:
            if os.path.getsize(_data_dir + '/' + item) < 10:
                continue
            readDf = pd.read_csv(
                _data_dir + '/' + item,
                encoding="utf_8_sig", sep=",",
                header=0,
                dtype={
                'market':str,
                'link':str,
                'price':str,
                'name':str, 
                'made':str, 
                'rarity':str, 
                'expansion':str, 
                'cn':str, 
                'type':str, 
                'mirror':str,
                'date':str,
                'datetime':str
                })
            if "price" not in readDf.columns:
                print('none price field:'+item)
                continue
            df = pd.concat([readDf, df], ignore_index=True,axis=0,sort=False)
        df = df.sort_values(by=['datetime'], ascending=False) 
        df = df.fillna('n/a')
        df = df[~df.duplicated(subset=['market','date','name','cn','type'],keep='first')]
        return df
    
class buyerCardrushBot():
    def download(self, drvWrapper, out_dir:str):
        # カード一覧へ移動
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        searchCsv = cardrushBuyCsv(out_dir)
        url = self.getResultPage(drvWrapper.getDriver())
        try:
            #drvWrapper.getWait().until(EC.visibility_of_all_elements_located((By.CLASS_NAME,'itemlist_box')))
            time.sleep(30)
            listHtml = drvWrapper.getDriver().page_source.encode('utf-8')
            parser = cardrushListParser(listHtml,url)
            l = parser.getItemList()
            for item in l:
                searchCsv.add(item)
                print(item)
            searchCsv.save()
        except TimeoutException as e:
            print("TimeoutException")
        except Exception as e:
            print(traceback.format_exc())
        
    def getResultPage(self, driver):
        url = 'https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vQT3Q9qDbZUpnP3_WH2I5qw8O-U_PqXVhhoIzH2o-tSzeDND9FTuoGKbZiNHTbrzTgKAUA2_SvXFh_2/pubhtml/sheet?headers=true&gid=1640929383&range=A1:G1000'
        try:
            driver.get(url)
        except WebDriverException as e:
            print("WebDriverException")
        except Exception as e:
            print(traceback.format_exc())
        return url
