import sqlite3
from Exchange import NSE, BSE
import datetime
from datetime import *
import sys


def distinct_positions(connector, cursor):
    # create Quote Table
    update_build_flag = __table_exists("Quotes", [], connector, cursor)
    if update_build_flag == 0:  # build
        fields = [('Date', 'text'), ('Ticker', 'text'), ('Exchange', 'text'), ('Close', 'real')]
        __table_exists("Quotes", fields, connector, cursor)
        res = cursor.execute('''SELECT Ticker, min(Date),max(Date) from Portfolio group by Ticker''').fetchall()
    else:
        res = cursor.execute('''select * from (select p.Ticker, max(p.min_Date,q.max_Date) as min_Date, p.max_Date as 
        max_Date from (SELECT Ticker, min(Date) as min_Date,max(Date)as max_Date from Portfolio group by Ticker)p, 
        (SELECT Ticker, max(Date) as max_Date from Quotes group by Ticker)q on p.Ticker = q.Ticker) where min_Date < 
        max_Date''').fetchall()
        pos = [(k[0], (datetime.strptime(k[1][:10], "%Y-%m-%d") + timedelta(days=1)),
                datetime.strptime(k[2][:10], "%Y-%m-%d") + timedelta(days=1)) for k in res]

    # Build a list of unique positions, starting and ending date
    # datetime.strptime(row[1][:10], '%Y-%m-%d')
    DBConnDetails = (connector, cursor)
    for _ in pos:  # each _ in the format of (Ticker, FromDate, ToDate)
        res = cursor.execute('''SELECT Ticker, NSE,BSE from SymbolMap where Ticker=:ticker''',
                             {'ticker': _[0]}).fetchall()
        for row in res:  # result of querying symbol map : Each row here is in the format (Ticker, NSECode, BSECode)
            ticker = row[0]
            if row[1] is not None and row[2] is not None:
                # Both Symbols exist
                TickerDet = (ticker, row[2], _[1], _[2])
                status = fetchBSEprices(TickerDet, DBConnDetails)
                if status == -1:
                    TickerDet = (ticker, row[1], _[1], _[2])
                    status = fetchNSEprices(TickerDet, DBConnDetails)
                    if status == -1:
                        sys.stderr.write(
                            "Couldn't fetch prices from BSE or NSE. Please check the ticker {}".format(_[0]))


            elif row[1] is not None and row[2] is None:
                # Only NSE exists
                TickerDet = (ticker, row[1], _[1], _[2])
                status = fetchNSEprices(TickerDet, DBConnDetails)
                if status == -1:
                    sys.stderr.write("\nCouldn't fetch prices from NSE. Please check the ticker {}".format(_[0]))

            elif row[1] is None and row[2] is not None:
                # only BSE exists
                TickerDet = (ticker, row[2], _[1], _[2])
                status = fetchBSEprices(TickerDet, DBConnDetails)
                if status == -1:
                    sys.stderr.write("\nCouldn't fetch prices from BSE. Please check the ticker {}".format(_[0]))
            else:
                # catch this error
                sys.stderr.write("\nNo symbol details in the Symbol Map. Please check the exchange traded tickers for {}".format(_[0]))
                pass


def fetchBSEprices(TickerDet, ConnDet):
    assert isinstance(TickerDet, tuple), "Expected Ticker Details Tuples"
    assert isinstance(ConnDet, tuple), "Expected Connection Tuples"
    assert (len(TickerDet) == 4), "Expected 4 values in ticker, received " + str(len(TickerDet))
    assert (len(ConnDet) == 2), "Expected 2 values in connection tuple, received " + str(len(ConnDet))
    ticker = TickerDet[0]
    exch_code = TickerDet[1]
    fromdate = TickerDet[2]
    todate = TickerDet[3]
    connector = ConnDet[0]
    cursor = ConnDet[1]

    exch = BSE()
    status, prices = exch.fetchbulkprices(exch_code, fromdate, todate)
    if status == -1:
        return status
    else:
        for k in prices.keys():
            cursor.execute('''INSERT INTO Quotes(Date,Ticker,Exchange, Close) VALUES(?,?,?,?)''',
                           (datetime.strptime(k, "%d-%B-%Y"), ticker, "BSE", prices[k]))
        connector.commit()
    return 0


def fetchNSEprices(TickerDet, ConnDet):
    assert isinstance(TickerDet, tuple), "Expected Ticker Details Tuples"
    assert isinstance(ConnDet, tuple), "Expected Connection Tuples"
    assert (len(TickerDet) == 4), "Expected 4 values in ticker, received " + str(len(TickerDet))
    assert (len(ConnDet) == 2), "Expected 2 values in connection tuple, received " + str(len(ConnDet))
    ticker = TickerDet[0]
    exch_code = TickerDet[1]
    fromdate = TickerDet[2]
    todate = TickerDet[3]
    connector = ConnDet[0]
    cursor = ConnDet[1]
    exch = NSE()
    # date splitting logic
    if todate > (fromdate + timedelta(days=364)):
        modified_fromdate = fromdate
        while modified_fromdate < todate:
            modified_todate = modified_fromdate + timedelta(days=364)
            status, prices = exch.fetchbulkprices(exch_code, modified_fromdate.strftime("%d-%m-%Y"),
                                                  modified_todate.strftime("%d-%m-%Y"))
            if status == -1:
                return status
            else:
                for k in prices.keys():
                    cursor.execute('''INSERT INTO Quotes(Date,Ticker,Exchange, Close) VALUES(?,?,?,?)''',
                                   (datetime.strptime(k, "%Y-%m-%d"), ticker, "NSE", prices[k]))
                    connector.commit()
            modified_fromdate = modified_fromdate + timedelta(days=365)
    else:
        status, prices = exch.fetchbulkprices(exch_code, fromdate.strftime("%d-%m-%Y"),
                                              todate.strftime("%d-%m-%Y"))
        if prices is None:
            return status

        for k in prices.keys():
            cursor.execute('''INSERT INTO Quotes(Date,Ticker,Exchange, Close) VALUES(?,?,?,?)''',
                           (datetime.strptime(k, "%Y-%m-%d"), ticker, "NSE", prices[k]))
            connector.commit()
        return status


def __table_exists(tablename, fields, connection, cursor):
    row = cursor.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                         {'type_name': 'table', 'table': tablename}).fetchall()
    if len(row) == 0:
        if len(fields) != 0:
            sys.stderr.write('No such table exists in the database.\n')
            sys.stderr.write('Creating a new table in the database.\n')
            fields = [i[0] + ' ' + i[1] for i in fields]
            fields = ','.join(fields)

            cursor.execute(
                '''CREATE  TABLE ''' + tablename + ''' (''' + fields + ''')''')
            connection.commit()
        return 0
    else:
        return 1
