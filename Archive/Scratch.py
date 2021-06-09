import requests
from bs4 import BeautifulSoup as bs
PULL_READ = False

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
if PULL_READ:
    session = requests.Session()
    url_symb_count = "https://www1.nseindia.com/marketinfo/sym_map/symbolCount.jsp?symbol=BETA"
    response = requests.get(url_symb_count, headers = headers)
    cookies = dict(response.cookies)
    x = (response.text).strip()
    f = {"segmentLink": "3","symbolCount": x,"series": "ALL","dateRange": " ","fromDate": "15-01-2021","toDate": "10-02-2021","dataType": "PRICEVOLUMEDELIVERABLE", "symbol": "BETA"}
    url_quotes = 'https://www1.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?'
    response = session.get(url_quotes,params = f, headers = headers, cookies=cookies)
    x = response.text
    file_dump = open('sample_html.txt', 'w+')
    file_dump.write(x)
    file_dump.close()
else:
    file_dump = open('sample_html.txt', 'r')
    x = file_dump.read(-1)
    file_dump.close()

soup = bs(x, 'html.parser')
table = soup.table
prices_dict={}
prices = []
for row in table.find_all('tr')[1:]:
    for datarow in row.find_all(attrs= {'class':'date'}):
        dt = datarow.get_text()
    for datarow in row.find_all(attrs={'class':'number'}):
        prices.append(datarow.get_text())
    prices_dict[dt]=prices[6]
    prices = []

print(prices_dict)






