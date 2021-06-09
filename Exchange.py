import datetime
import sys
import requests
from bs4 import BeautifulSoup as bs
from abc import ABC,abstractmethod
class Exchange(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def fetchbulkprices(self,ticker,fromdate,todate):
        pass

    @abstractmethod
    def fetchlatestprices(self,ondate):
        pass

class NSE(Exchange):
    def __init__(self):
        self.url_symb_count = "https://www1.nseindia.com/marketinfo/sym_map/symbolCount.jsp?"
        self.url_quotes = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?'

        return
    def fetchlatestprices(self,ondate):
        return NotImplemented

    def fetchbulkprices(self,ticker, fromdate, todate):
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Dnt": "1",
            "Connection": "keep-alive",
            "Host": "www1.nseindia.com",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://www1.nseindia.com/products/content/equities/equities/eq_security.htm",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        session = requests.Session()
        params = {"symbol":ticker}
        response = requests.get(self.url_symb_count,params = params, headers=headers)
        cookies = dict(response.cookies)
        x = (response.text).strip()
        params = {"segmentLink": "3", "symbolCount": x, "series": "ALL", "dateRange": " ", "fromDate": fromdate,
             "toDate": todate, "dataType": "PRICEVOLUMEDELIVERABLE", "symbol": ticker}
        response = session.get(self.url_quotes, params=params, headers=headers, cookies=cookies)
        file_dump = open('sample_html.txt', 'w+')
        file_dump.write(response.text)
        file_dump.close()
        soup = bs(response.text, 'html.parser')
        prices_dict = {}
        csvdata = []
        for d in soup.find_all(attrs={'id':'csvContentDiv'}):
            csvdata.append(d.get_text())
        csvdata = csvdata[0]
        csvdata = csvdata.split(':')
        for i in range(1, len(csvdata) - 1):
            dailydata = csvdata[i].split(',')
            prices_dict[dailydata[2].strip('" ')] = dailydata[9].strip('" ')
        return prices_dict
