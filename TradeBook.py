import sys
import sqlite3
import openpyxl
import os

class TradeBook():
    def __init__(self):
        self.trades = []
        self.cursor = None
        return

    def readtrades(self,path,filename):
        # reads trades from wbk and stores it in local memory (as a list of tuples)
        try:
            wb = openpyxl.load_workbook(os.path.join(path, filename))
        except:
            return False

        ws = wb['TradeBook']
        for row in ws.iter_rows(min_row=2, max_col=5, max_row=21, values_only=True):
            self.trades.append(tuple(row))
        return True

    def write_db(self,db_path,db_filename):
        #takes a list of tuples, writes into db
        try:
            connexion = sqlite3.connect(os.path.join(db_path, db_filename))
            self.cursor = connexion.cursor()
            if len(self.trades) == 0:
                sys.stderr.write('CAUTION!:\nNo prior trades loaded')
                raise ValueError

            trade_fields = [('TradeDate', 'text'), ('Ticker', 'text'), ('Action', 'text'), ('Qty', 'integer'), ('Price', 'real')]
            self.__table_exists('TradeBook',trade_fields)

            for i in range(len(self.trades)):
                self.cursor.execute(
                    '''INSERT INTO TradeBook ('TradeDate','Ticker','Action','Qty','Price') VALUES (?,?,?,?,?)''',
                    self.trades[i])
            connexion.commit()

        except sqlite3.OperationalError:
            sys.stderr.write('CAUTION!:\nEither the database doesn\'t exist or the path is improper')
            return
        except ValueError:
            sys.stderr.write('No prior trades loaded and file path and name offered is invalid')
            return



    def __table_exists(self, tablename,fields):

        row = self.cursor.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                          {'type_name': 'table', 'table': tablename}).fetchall()
        if len(row) == 0:
            sys.stderr.write('No such table exists in the database.\n')
            sys.stderr.write('Creating a new table in the database.\n')
            fields = [i[0] + ' ' + i[1] for i in fields]
            fields = ','.join(fields)

            self.cursor.execute(
                '''CREATE  TABLE ''' + tablename + ''' ('''+ fields + ''')''')
            self.cursor.commit()

        return
