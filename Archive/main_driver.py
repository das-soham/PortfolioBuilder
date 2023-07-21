import Quotes
import SymbolMap
import TradeBook
import Portfolio
from Exchange import BSE
from Quotes import *
from datetime import date
from EquityCurve import Equity
import sqlite3
import os

# STEP 1: Read Trades; Write Trades into DB

#trade_book = TradeBook.TradeBook('K:\/','Portfolio_Check.db','TradeBook')
#trade_book.readtrades('K:\/','Tradebook.xlsx')
#trade_book.write_db()
#print(trade_book.min('Ticker'))

# STEP 2: Build Portfolio for every day
#portfolio = Portfolio.Portfolio()
#portfolio.build('TradeBook',trade_book,db_path='K:\/',db_name='Portfolio_Check.db')
#portfolio.write_db(db_path='K:\/',db_name='Portfolio_Check.db')

#STEP 3: Populate the Symbol Map Table to map the positions with appropriate NSE/BSE tickers
#symbolmap = SymbolMap.SymbolMap()
#symbolmap.readmap('K:\/','Tradebook.xlsx','Symbol Map')
#symbolmap.writemap('K:\/','Portfolio_Check.db')


#Step 4: Find distinct positions from Portfolio, start date, end date and the appropriate ticker to fetch quotes, populate in Quotes table

connexion = sqlite3.connect(os.path.join('K:\/','Portfolio_Check.db'))
Quotes.distinct_positions(connexion,connexion.cursor())

#Step 5: Using Daily Portfolio and Daily Quotes, build the daily ending value of Portfolio, and write it in Equity table
#strategy = Equity(connexion,connexion.cursor())
#strategy.build()

#Ancillary Utilities - Connect with a particular Equity curve and fetch its value at a certain date
#strategy.load()
#val_dict = strategy.__ondate__((datetime(2023,3,27,00,00,00)))
pass
