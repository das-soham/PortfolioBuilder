import TradeBook
import Portfolio
from Quotes import *
import sqlite3
import os
#portfolio = Portfolio.Portfolio()
#portfolio.build('TradeBook',db_path='K:\/',db_name='Portfolio.db')
#portfolio.write_db(db_path='K:\/',db_name='Portfolio.db')
#print(portfolio.portfolio)

connexion = sqlite3.connect(os.path.join('K:\/','Portfolio.db'))
distinct_positions(connexion,connexion.cursor())


