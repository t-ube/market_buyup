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

class hareruya2CodeConverter():
    def getItemTitle(self,_BeautifulSoup):
        div = _BeautifulSoup.find("div", class_="table_left_cell")
        if div is not None:
            return div.get_text()
        return None
    
    def getPrice(self,_BeautifulSoup):
        div = _BeautifulSoup.find("div", class_="table_right_cell")
        if div is not None:
            return div.get_text()
        return None
    
    def getMirror(self, title:str):
        find_pattern = r":ミラー"
        m = re.search(find_pattern, title)
        if m != None:
            return 1
        return 0
    
    def getNameFromTitle(self,title:str):
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)〉\[(?P<x>.+)\]"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('n')
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('n')
        return None

    def getRarityFromTitle(self,title:str):
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)〉\[(?P<x>.+)\]"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('r')
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('r')
        return ''
    
    def getCNFromTitle(self,title:str):
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)〉\[(?P<x>.+)\]"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('c')
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('c')
        return ''
    
    def getExpansionFromTitle(self,title:str):
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)〉\[(?P<x>.+)\]"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('x')
        find_pattern = r"(?P<n>.+)\((?P<r>.+)\)(?P<t>{.+})〈(?P<c>.+/.+)"
        m = re.search(find_pattern, title)
        if m != None:
            return self.getExpansionFromCN(m.group('c'))
        return ''
    
    def getExpansionFromCN(self,cn:str):
        find_pattern = r".+/(?P<x>.+)"
        m = re.search(find_pattern, cn)
        if m != None:
            return m.group('x')
        return ''

class hareruya2ListParser():
    def __init__(self, _html, _url):
        self.__html = _html
        self.__url = _url

    def getItemList(self):
        converter = hareruya2CodeConverter()
        soup = BeautifulSoup(self.__html, 'html.parser')
        l = list()
        tableList = soup.find_all("div", class_="table_main")
        for table in tableList:
            title = converter.getItemTitle(table)
            if title is None:
                continue
            price = converter.getPrice(table)
            if price == None:
                continue     
            name = converter.getNameFromTitle(title)
            if name is None:
                continue
            l.append({
                "market": 'hareruya2',
                "link": self.__url,
                "price": int(re.findall('[0-9]+', price.replace(',',''))[0]),
                "name": name,
                "rarity": converter.getRarityFromTitle(title),
                "expansion": converter.getExpansionFromTitle(title),
                "cn": converter.getCNFromTitle(title),
                "mirror": converter.getMirror(title),
                "date": None,
                "datetime": None
            })
        return l

class hareruya2BuyCsv():
    def __init__(self,_out_dir,_page_no):
        dt = jst.now().replace(microsecond=0)
        self.__out_dir = _out_dir
        self.__page_no = _page_no
        self.__list = list()
        self.__date = str(dt.date())
        self.__datetime = str(dt)
        self.__file = _out_dir+'/'+self.__datetime.replace("-","_").replace(":","_").replace(" ","_")+'_hareruya2_'+str(_page_no)+'.csv'

    def init(self):
        labels = [
         'market',
         'link',
         'price',
         'name', 
         'rarity', 
         'expansion', 
         'cn', 
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

class hareruya2BuyCsvLoader():
    def load(self, _data_dir):
        df = pd.DataFrame(index=[], columns=[
            'market',
            'link',
            'price',
            'name', 
            'rarity', 
            'expansion', 
            'cn', 
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
                'rarity':str, 
                'expansion':str, 
                'cn':str, 
                'mirror':str,
                'date':str,
                'datetime':str
                })
            if "price" not in readDf.columns:
                print('none price field:'+item)
                continue
            df = pd.concat([readDf, df], ignore_index=True,axis=0,sort=False)
        df = df.sort_values(by=['datetime','price'], ascending=[False,False])
        df = df.fillna('n/a')
        df = df[~df.duplicated(subset=['market','date','expansion','cn','mirror'],keep='first')]
        return df

class buyerHareruya2Bot():
    def download(self, drvWrapper, page_no:int, out_dir:str):
        # カード一覧へ移動
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        searchCsv = hareruya2BuyCsv(out_dir,page_no)
        url = self.getResultPage(drvWrapper.getDriver(), page_no)
        try:
            #drvWrapper.getWait().until(EC.visibility_of_all_elements_located((By.CLASS_NAME,'page_button_container')))
            time.sleep(4)
            listHtml = drvWrapper.getDriver().page_source.encode('utf-8')
            parser = hareruya2ListParser(listHtml,url)
            l = parser.getItemList()
            for item in l:
                searchCsv.add(item)
                print(item)
            searchCsv.save()
        except TimeoutException as e:
            print("TimeoutException")
        except Exception as e:
            print(traceback.format_exc())
        
    def getResultPage(self, driver, page_no):
        url = 'https://www.hareruya2.com/page/'+str(page_no)
        try:
            driver.get(url)
        except WebDriverException as e:
            print("WebDriverException")
        except Exception as e:
            print(traceback.format_exc())
        return url
