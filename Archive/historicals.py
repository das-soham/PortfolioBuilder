import copy
import json
import os
import sqlite3
import sys
from datetime import *

import openpyxl
from dateutil.parser import *
from dateutil.rrule import *

from Exchange import NSE


def connect_db(name, path):
    try:
        connexion = sqlite3.connect(os.path.join(path,name))
        return (connexion,None)
    except sqlite3.OperationalError:
        sys.stderr.write('CAUTION!:\nEither the database doesn\'t exist or the path is improper')
        return (None,'Error')

def DB_Empty(connexion):
    cur = connexion.cursor()
    row  = cur.execute("SELECT name FROM sqlite_master where type =\'table\'").fetchall()
    if len(row)==0:
        sys.stderr.write('No table/tables exist in the database\n')
        return True
    else:
        return False

def Table_Exists(connexion,table_name):
    cur = connexion.cursor()
    row  = cur.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",{'type_name':'table','table':table_name}).fetchall()
    if len(row)==0:
        sys.stderr.write('No such table exists in the database.\n')
        sys.stderr.write('Creating a new table in the database.\n')
        cur.execute('''CREATE  TABLE ''' + table_name + ''' (TradeDate text, Ticker text, Action text, Qty integer, Price real)''')
        connexion.commit()
        return False
    else:
        return True

def Read_Store_Trades(connexion,file_name,path):
    wb = openpyxl.load_workbook(os.path.join(path,file_name))
    ws = wb['TradeBook']
    pf = []
    for row in ws.iter_rows(min_row=2, max_col=5, max_row=21,values_only=True):
        pf.append(tuple(row))
    cur = connexion.cursor()
    for i in range(len(pf)):
        cur.execute('''INSERT INTO TradeBook ('TradeDate','Ticker','Action','Qty','Price') VALUES (?,?,?,?,?)''',pf[i])
    connexion.commit()


''' Reads the trades from SQL, converts it into a portfolio saves it in a text file'''
def PortfolioBuilder(connexion,filename):
    date_range = list(rrule(DAILY, dtstart = parse("2020-09-18 00:00:00"),until=datetime.now().date(), byweekday=range(5)))
    portfolio_dict={}
    positions_dict = {}
    cursor = connexion.cursor()
    for i in range(len(date_range)):
        if portfolio_dict.get(date_range[i - 1]) is not None:
            positions_dict = portfolio_dict[date_range[i - 1].strftime("%Y-%m-%d 00:00:00")]
        res = cursor.execute('''SELECT * from TradeBook where TradeDate =:date_time''',{'date_time': date_range[i].strftime("%Y-%m-%d 00:00:00")}).fetchall()
        for row in res:
            if positions_dict.get(row[1]) is None:
                positions_dict[row[1]] = (row[3], row[4])
            else:
                if row[2]=='Buy':
                    wtd_price = round((positions_dict[row[1]][0]*positions_dict[row[1]][1] + row[3]*row[4])/(positions_dict[row[1]][0] + row[3]),2)
                    positions_dict[row[1]] =(positions_dict[row[1]][0] + row[3], wtd_price)
                else:
                    positions_dict[row[1]] = (positions_dict[row[1]][0] - row[3], positions_dict[row[1]][1])
        portfolio_dict[date_range[i].strftime("%Y-%m-%d 00:00:00")] = copy.deepcopy(positions_dict)
    f = open(filename,'w+')
    json.dump(portfolio_dict,f)
    f.close()


''' Loads the Portfolio from textfile to RAM'''
def Asset_Builder(filename):
    f = open(filename)
    portfolio_dict = {}
    portfolio_dict_temp = json.load(f)
    for k in portfolio_dict_temp.keys():
        d = parse(k)
        portfolio_dict[d]=portfolio_dict_temp[k]

    return portfolio_dict

def PositionTiming(connexion):
    cursor = connexion.cursor()
    exch = NSE()
    distinct_pos = cursor.execute(''' SELECT Ticker, min(Date) as [Entry], max(Date) as [Exit] from Portfolio group by Ticker ''')
    for row in distinct_pos:
        ticker = row(0)
        fromdate = row(1)
        todate = row(2)
        prices = exch.fetchbulkprices(ticker,fromdate,todate)
    return



if __name__ =='__main__':
    #conn,status = connect_db('Portfolio.db','K:\/')
    #DB_Empty(conn)
    #status = Table_Exists(conn,'TradeBook')
    #Read_Store_Trades(conn,'TradeBook.xlsx','K:\/')
    #PortfolioBuilder(conn,os.path.join('K:\/','Portfolio.txt'))
    #pf = Asset_Builder(os.path.join('K:\/','Portfolio.txt'))
    #writer = PortfolioWriter('K:\/','Portfolio.db')
    #writer.WritePortfolio(pf)

    #exch = NSE()
    #prices = exch.fetchbulkprices('AAVAS','18-09-2020','08-06-2021')
    #exch.fetchcsv()
    #print(prices)

