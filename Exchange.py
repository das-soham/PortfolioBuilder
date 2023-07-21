import csv
import datetime
import sys
import requests
from bs4 import BeautifulSoup as bs
from abc import ABC, abstractmethod


class Exchange(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def fetchbulkprices(self, ticker, fromdate, todate):
        pass

    @abstractmethod
    def fetchlatestprices(self, ondate):
        pass


class NSE(Exchange):
    def __init__(self):
        self.url_initial = "https://www.nseindia.com/get-quotes/equity"
        self.url_quotes = 'https://www.nseindia.com/api/historical/cm/equity'

        return

    def fetchlatestprices(self, ondate):
        return NotImplemented

    def fetchbulkprices(self, ticker, fromdate, todate):
        sys.stderr.write("Querying Ticker = {} fromdate = {} todate {} \n".format(ticker, fromdate, todate))

        headers = {
            "Authority": "www.nseindia.com",
            "Method": "GET",
            #"Path": "/api/historical/cm/equity?symbol=BETA&from=21-01-2023&to=01-07-2023",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate,br",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Sec-Ch-Ua": "Not.A/Brand;v = 8, Chromium;v = 114, Microsoft Edge;v = 114",
            "Scheme": "https",
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "Windows",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        session = requests.Session()
        params = {"symbol": ticker}
        response = requests.get(self.url_initial, params=params, headers=headers)
        cookies = dict(response.cookies)
        params = {"symbol": ticker, "from": fromdate,"to": todate}
        response = session.get(self.url_quotes, params=params, headers=headers, cookies=cookies)
        prices_dict = {}
        if response.status_code == 200:
            payload = response.json()['data']
            for entry in payload:
                prices_dict[entry["CH_TIMESTAMP"]]= entry["CH_CLOSING_PRICE"]
            return 0, prices_dict
        else:
            sys.stderr.write("Couldn't fetch data for {}".format(ticker))
            return -1, None


class BSE(Exchange):
    def __init__(self):
        self.csv_url = 'https://api.bseindia.com/BseIndiaAPI/api/StockPriceCSVDownload/w?'
        self.cookie_url = 'https://api.bseindia.com/BseIndiaAPI/api/StockpricesearchData/w?'\

    def fetchlatestprices(self, ondate):
        return NotImplemented

    def fetchbulkprices(self, ticker, fromdate, todate):
        sys.stderr.write("Querying Ticker = {} fromdate = {} todate {} \n".format(ticker, fromdate, todate))
        prices_dict = {}

        headers = {
            "authority": "api.bseindia.com",
            "method": "GET",
            "path": "",
            "scheme": "https",
            "accept": "application / json, text / plain, * / *",
            "accept-Encoding": "gzip, deflate, br",
            "accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "referer": "https://www.bseindia.com",
            "sec-ch-ua": "Google Chrome" + ";" + "v=""111"", ""Not(A:Brand""" + ";" + "v=""8""" + ",""Chromium""",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        }
        params = {"MonthDate": fromdate.strftime('%d/%m/%Y'), "Scode": ticker, "YearDate": todate.strftime('%d/%m/%Y'), "pageType": 0, "rbType": "D"}
        session = requests.Session()
        response = requests.get(self.cookie_url, params=params, headers=headers)
        cookies = response.cookies
        params = {"pageType": 0, "rbType": "D", "Scode": ticker, "FDates": fromdate.strftime('%d/%m/%Y'), "TDates": todate.strftime('%d/%m/%Y')}
        response = session.get(self.csv_url, params=params, headers=headers, cookies=cookies)
        if response.status_code == 200:
            sys.stderr.write("Queried successfully \n")
            response_content = response.content.decode('utf-8')
            csv_data = csv.reader(response_content.splitlines(),delimiter=',')
            csv_data = list(csv_data)
            for i in range(1, len(csv_data)):   # why -1?
                dailydata = csv_data[i]
                prices_dict[dailydata[0]] = dailydata[4]
            return 0, prices_dict
        else:
            sys.stderr.write("Couldn't fetch data for {}".format(ticker))
            return (-1,)
