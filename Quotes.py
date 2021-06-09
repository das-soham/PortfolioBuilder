import sqlite3
from Exchange import NSE
from datetime import *
def distinct_positions(connector,cursor):
    res = cursor.execute('''SELECT Ticker, min(Date),max(Date) from Portfolio group by Ticker''').fetchall()
    pos = []
    for row in res:
        pos.append((row[0],datetime.strptime(row[1][:10], '%Y-%m-%d'),datetime.strptime(row[2][:10], '%Y-%m-%d')))



    exch = NSE()
    for p in pos:
        prices = exch.fetchbulkprices(p[0], p[1].strftime("%d-%m-%Y"), p[2].strftime("%d-%m-%Y"))
        for k in prices.keys():
            cursor.execute('''INSERT INTO Quotes(Date,Ticker,Close) VALUES(?,?,?)''',(datetime.strptime(k,"%d-%b-%Y"),p[0],prices[k]))
        connector.commit()
    return

