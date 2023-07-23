import Quotes
import SymbolMap
import TradeBook
import Portfolio
from EquityCurve import Equity
import sqlite3
import os

BOOK_PATH = 'K:\/'
BOOK_NAME = 'Tradebook.xlsx'
DB_PATH = 'K:\/'
DB_NAME = 'Portfolio_Check.db'
# STEP 1: Read Trades; Write Trades into DB

trade_book = TradeBook.TradeBook(DB_PATH,DB_NAME,'TradeBook')
trade_book.readtrades(BOOK_PATH,BOOK_NAME)
trade_book.write_db()


# STEP 2: Build Portfolio for every day
portfolio = Portfolio.Portfolio(db_path= DB_PATH,db_name=DB_NAME)
portfolio.build('TradeBook',trade_book)
portfolio.write_db(DB_PATH,DB_NAME)

#STEP 3: Populate the Symbol Map Table to map the positions with appropriate NSE/BSE tickers
#symbolmap = SymbolMap.SymbolMap()
#symbolmap.readmap(BOOK_PATH, BOOK_NAME, 'Symbol Map')
#symbolmap.writemap(DB_PATH,DB_NAME)


#Step 4: Find distinct positions from Portfolio, start date, end date and the appropriate ticker to fetch quotes, populate in Quotes table

connexion = sqlite3.connect(os.path.join(DB_PATH,DB_NAME))
#Quotes.distinct_positions(connexion,connexion.cursor())

#Step 5: Using Daily Portfolio and Daily Quotes, build the daily ending value of Portfolio, and write it in Equity table
strategy = Equity(connexion,connexion.cursor())
strategy.alter_table(portfolio.update_from)
strategy.net_infusion(None, None)
strategy.NAV_Qty()
strategy.equity_curve()

#Ancillary Utilities - Connect with a particular Equity curve and fetch its value at a certain date
#strategy.load()
#val_dict = strategy.__ondate__((datetime(2023,3,27,00,00,00)))
