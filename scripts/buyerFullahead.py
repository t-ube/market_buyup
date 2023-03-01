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

class fullaheadCodeConverter():
    def getCNFromCode(self, code):
        find_pattern = r"pk-(?P<x>.+)-m(?P<c>[0-9a-zA-Z]+)"
        m = re.search(find_pattern, code)
        if m != None:
            return m.group('c')
        find_pattern = r"pk-(?P<x>.+)-(?P<c>[0-9a-zA-Z]+)"
        m = re.search(find_pattern, code)
        if m != None:
            return m.group('c')
        return ''
    
    def _getExpansionFromCode(self, code):
        find_pattern = r"pk-(?P<x>.+)-(?P<c>[0-9a-zA-Z]+)"
        m = re.search(find_pattern, code)
        if m != None:
            return m.group('x')
        return ''
    
    def _getOfficialExpansionFromExpansion(self, expansion):
        find_pattern = r"(?P<p1>[a-zA-Z-]+)(?P<p2>[0-9]+)(?P<p3>[a-zA-Z-]*)"
        m = re.search(find_pattern, expansion)
        if m != None:
            return m.group('p1')+str(int(m.group('p2')))+m.group('p3')
        return expansion
    
    def getOfficialExpansionFromCode(self, code):
        return self._getOfficialExpansionFromExpansion(self._getExpansionFromCode(code))

    def getMirror(self, code):
        find_pattern = r"pk-(?P<x>.+)-m(?P<c>[0-9a-zA-Z]+)"
        m = re.search(find_pattern, code)
        if m != None:
            return 1
        return 0

class fullaheadListParser():
    def __init__(self, _html, _url):
        self.__html = _html
        self.__url = _url

    def getItemList(self):
        converter = fullaheadCodeConverter()
        soup = BeautifulSoup(self.__html, 'html.parser')
        due = self.getDueDate(soup)
        l = list()
        liList = soup.find_all("li")
        for li in liList:
            title = self.getItemTitle(li)
            if title is None:
                continue
            name = self.getNameFromTitle(title)
            if name is None:
                continue
            code = self.getCode(li)
            if code is None:
                continue 
            price = self.getPrice(li)
            if price == None:
                continue
            l.append({
                "market": 'fullahead',
                "link": self.__url,
                "code": code,
                "price": int(re.findall('[0-9]+', price.replace(',',''))[0]),
                "name": name,
                "rarity": self.getRarityFromTitle(title),
                "expansion": converter.getOfficialExpansionFromCode(code),
                "cn": converter.getCNFromCode(code),
                "mirror": converter.getMirror(code),
                "date": None,
                "datetime": None,
                "due": due,
            })
        return l
    
    def getPrice(self,_BeautifulSoup):
        p = _BeautifulSoup.find("p", class_="price")
        if p is not None:
            return p.get_text()
        return None
    
    def getItemTitle(self,_BeautifulSoup):
        div = _BeautifulSoup.find("div", class_="title")
        if div == None:
            return None
        h3 = div.find("h3")
        if h3 is not None:
            return h3.get_text()
        return None

    def getNameFromTitle(self,title:str):
        find_pattern = r"[0-9a-zA-Z]+-[0-9a-zA-Z]+-[0-9a-zA-Z]+ (?P<n>.+) ([a-zA-Z]+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('n')
        find_pattern = r"[0-9a-zA-Z]+-[0-9a-zA-Z]+-[0-9a-zA-Z]+ (?P<n>.+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('n')
        return None

    def getRarityFromTitle(self,title:str):
        find_pattern = r"[0-9a-zA-Z]+-[0-9a-zA-Z]+-[0-9a-zA-Z]+ (.+) (?P<r>[a-zA-Z]+)"
        m = re.search(find_pattern, title)
        if m != None:
            return m.group('r')
        return ''
    
    def getDueDate(self,_BeautifulSoup):
        div = _BeautifulSoup.find("div", class_="editor_front")
        if div == None:
            return None
        spanList = div.find_all("span")
        for span in spanList:
            date = span.get_text()
            find_pattern = r"(?P<m>[0-9]+)/(?P<d>[0-9]+)"
            m = re.search(find_pattern, date)
            if m != None:
                dt_now = datetime.datetime.now()
                year = dt_now.year
                if int(m.group('m')) < dt_now.month:
                    year += 1
                d = datetime.date(year, int(m.group('m')), int(m.group('d')))
                return d.strftime('%Y-%m-%d')
        return None
    
    def getCode(self,_BeautifulSoup):
        div = _BeautifulSoup.find("div", class_="title")
        if div == None:
            return None
        p = div.find("p", class_="code")
        if p is not None:
            return p.get_text()
        return None


class fullaheadBuyCsv():
    def __init__(self,_out_dir,_subcat):
        dt = jst.now().replace(microsecond=0)
        self.__out_dir = _out_dir
        self.__subcat = _subcat
        self.__list = list()
        self.__date = str(dt.date())
        self.__datetime = str(dt)
        self.__file = _out_dir+'/'+self.__datetime.replace("-","_").replace(":","_").replace(" ","_")+'_fullahead_'+str(_subcat)+'.csv'

    def init(self):
        labels = [
         'market',
         'link',
         'code',
         'price',
         'name', 
         'rarity', 
         'expansion', 
         'cn', 
         'mirror',
         'date',
         'datetime',
         'due'
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

class fullaheadBuyCsvLoader():
    def load(self, _data_dir):
        df = pd.DataFrame(index=[], columns=[
            'market',
            'link',
            'code',
            'price',
            'name', 
            'rarity', 
            'expansion', 
            'cn', 
            'mirror',
            'date',
            'datetime',
            'due'
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
                'code':str,
                'price':str,
                'name':str, 
                'rarity':str, 
                'expansion':str, 
                'cn':str, 
                'mirror':str,
                'date':str,
                'datetime':str,
                'due':str
                })
            if "price" not in readDf.columns:
                print('none price field:'+item)
                continue
            df = pd.concat([readDf, df], ignore_index=True,axis=0,sort=False)
        df = df.sort_values(by=['datetime'], ascending=False) 
        df = df.fillna('n/a')
        df = df[~df.duplicated(subset=['market','date','code'],keep='first')]
        return df

class buyerFullaheadBot():
    def download(self, drvWrapper, subcat:int, out_dir:str):
        # カード一覧へ移動
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        searchCsv = fullaheadBuyCsv(out_dir,subcat)
        url = self.getResultPage(drvWrapper.getDriver(), subcat)
        try:
            #drvWrapper.getWait().until(EC.visibility_of_all_elements_located((By.CLASS_NAME,'image')))
            time.sleep(10)
            listHtml = drvWrapper.getDriver().page_source.encode('utf-8')
            parser = fullaheadListParser(listHtml,url)
            l = parser.getItemList()
            for item in l:
                searchCsv.add(item)
                print(item)
            searchCsv.save()
        except TimeoutException as e:
            print("TimeoutException")
        except Exception as e:
            print(traceback.format_exc())
        
    def getResultPage(self, driver, subcat):
        url = 'https://fullahead-buy.com/?shopbrand=pk&subcat='+str(subcat)
        try:
            driver.get(url)
        except WebDriverException as e:
            print("WebDriverException")
        except Exception as e:
            print(traceback.format_exc())
        return url
