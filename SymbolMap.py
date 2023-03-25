import sqlite3
import sys
import os
import copy
import openpyxl

class SymbolMap:
    def __init__(self):
        self.symbols = []
        return

    def readmap(self,path, filename, wks_name):
        try:
            wb = openpyxl.load_workbook(os.path.join(path, filename))
        except:
            return False
        ws = wb[wks_name]
        for row in ws.iter_rows(min_row=2, max_col=3, max_row=50, values_only=True):
            self.symbols.append(tuple(row))
        return True


    def writemap(self,db_path, db_filename):
        # takes a list of tuples, writes into db
        try:
            connexion = sqlite3.connect(os.path.join(db_path, db_filename))
            self.cursor = connexion.cursor()
            if len(self.symbols) == 0:
                sys.stderr.write('CAUTION!:\nNo symbol map loaded')
                raise ValueError

            symbolmap_fields = [('Ticker', 'text'), ('NSE', 'text'), ('BSE', 'text')]
            self.__table_exists('SymbolMap', symbolmap_fields, connexion)

            for i in range(len(self.symbols)):
                self.cursor.execute(
                    '''INSERT INTO SymbolMap ('Ticker','NSE','BSE') 
                    VALUES (?,?,?)''',
                    self.symbols[i])
            connexion.commit()

        except sqlite3.OperationalError:
            sys.stderr.write('CAUTION!:\nEither the database doesn\'t exist or the path is improper')
            return
        except ValueError:
            sys.stderr.write('No prior trades loaded and file path and name offered is invalid')
            return

    def __table_exists(self, tablename, fields, connector):

        row = self.cursor.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                                  {'type_name': 'table', 'table': tablename}).fetchall()
        if len(row) == 0:
            sys.stderr.write('No such table exists in the database.\n')
            sys.stderr.write('Creating a new table in the database.\n')
            fields = [i[0] + ' ' + i[1] for i in fields]
            fields = ','.join(fields)

            self.cursor.execute(
                '''CREATE  TABLE ''' + tablename + ''' (''' + fields + ''')''')
            connector.commit()

        return
