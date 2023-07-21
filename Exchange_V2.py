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
        self.url_landing = "https://www.nseindia.com/"
        self.url_quotes = "https://www.nseindia.com/api/historical/cm/equity?"

        return

    def fetchlatestprices(self, ondate):
        return NotImplemented

    def fetchbulkprices(self, ticker, fromdate, todate):
        sys.stderr.write("Querying Ticker = {} fromdate = {} todate {} \n".format(ticker, fromdate, todate))
        headers = {
            "authority": "www.nseindia.com",
            "method": "GET",
            "path": "/api/historical/cm/equity?symbol=" + ticker + "&series = [%22EQ%22]&from=" + fromdate + "&to=" + todate + "&csv=true",
            "scheme": "https",
            "accept": "*/*",
            "accept-Encoding": "gzip, deflate, br",
            "accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "referer": "https://www.nseindia.com/get-quotes/equity?symbol="+ticker,
            "sec-ch-ua": "Google Chrome" + ";" + "v=""111"", ""Not(A:Brand""" + ";" + "v=""8""" + ",""Chromium""",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
            "x -requested-with": "XMLHttpRequest"
        }
        session = requests.Session()
        with session:
            _ = session.get(self.url_landing, headers=headers)
            params = {"symbol": ticker, "from": fromdate, "to": todate}
            response = session.get(self.url_quotes, params=params, headers=headers)
        if response.status_code == 200:
            sys.stderr.write("Queried successfully")
            print(response.json())
            return response.json()
        else:
            sys.stderr.write("Couldn't fetch data for {}".format(ticker))
            sys.stderr.write(str(response.status_code))
            sys.stderr.write(response.text)
            return {}


if __name__ == '__main__':
    exch = NSE()
    exch.fetchbulkprices('AAACV2544H','24-03-2023','29-03-2023')

