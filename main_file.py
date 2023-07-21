import TradeBook
import Portfolio
from Quotes import *
import sqlite3
import os

connexion = sqlite3.connect(os.path.join('K:\/','Portfolio.db'))
distinct_positions(connexion,connexion.cursor())


