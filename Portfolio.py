import sqlite3
import sys
import os
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
import copy
class Portfolio():
    def __init__(self):
        self.portfolio = {}
        self.portfolio_table = 'Portfolio'
        self.positions = {}
        self.cursor = None
        self.connection = None
        return

    def build(self,trade_table,connector=None,db_path=None,db_name=None):
        try:
            if connector is None:
                if db_name is None:
                    raise ValueError
                else:
                    self.connection = sqlite3.connect(os.path.join(db_path, db_name))
                    self.cursor = self.connection.cursor()
            else:
                self.cursor = connector.cursor()
            res = self.cursor.execute('''select min(TradeDate) from '''+ trade_table).fetchall()
            date_range = list(rrule(DAILY, dtstart=parse(res[0][0]), until=datetime.now().date(), byweekday=range(5)))
            for i in range(len(date_range)):
                if self.portfolio.get(date_range[i - 1]) is not None:
                    self.positions = self.portfolio[date_range[i - 1].strftime("%Y-%m-%d 00:00:00")]
                res = self.cursor.execute('''SELECT * from ''' + trade_table + ''' where TradeDate =:date_time''',
                                     {'date_time': date_range[i].strftime("%Y-%m-%d 00:00:00")}).fetchall()
                for row in res:
                    if self.positions.get(row[1]) is None:
                        self.positions[row[1]] = (row[3], row[4])
                    else:
                        if row[2] == 'Buy':
                            wtd_price = round((self.positions[row[1]][0] * self.positions[row[1]][1] + row[3] * row[4]) / (self.positions[row[1]][0] + row[3]), 2)
                            self.positions[row[1]] = (self.positions[row[1]][0] + row[3], wtd_price)
                        else:
                            self.positions[row[1]] = (self.positions[row[1]][0] - row[3], self.positions[row[1]][1])
                self.portfolio[date_range[i].strftime("%Y-%m-%d 00:00:00")] = copy.deepcopy(self.positions)



        except ValueError:
            sys.stderr.write('NO CONNECTOR or DB NAME provided')
            return
        except sqlite3.OperationalError:
            sys.stderr.write('SQL error')
            return


        return

    def write_db(self, connection = None, db_path = None, db_name = None):
        try:
            if self.connection is None:
                if connection is None:
                    if db_name is None:
                        raise Exception
                    else:
                        self.connection = sqlite3.connect(os.path.join(db_path, db_name))
                        self.cursor = self.connection.cursor()
                else:
                    self.cursor = connection.cursor()

            if self.portfolio is None:
                raise ValueError

            fields = [('Date', 'text'), ('Ticker', 'text'), ('Price', 'real'), ('Qty', 'integer')]
            self.__table_exists(self.cursor,self.portfolio_table,fields)

            for k in self.portfolio.keys():
                for pos in self.portfolio[k].keys():
                    self.cursor.execute('''INSERT INTO Portfolio ('Date','Ticker','Price','Qty') VALUES (?,?,?,?)''',
                                (k, pos, self.portfolio[k][pos][1], self.portfolio[k][pos][0]))
            self.connection.commit()
            self.cursor.execute('''DELETE FROM ''' + self.portfolio_table  +''' where Qty = 0''')
            self.connection.commit()
            return
        except ValueError:
            sys.stderr.write('No prior portfolio loaded into memory')
            return


    def __table_exists(self, connector, tablename, fields):
        row = self.cursor.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                                  {'type_name': 'table', 'table': tablename}).fetchall()
        if len(row) == 0:
            sys.stderr.write('No such table exists in the database.\n')
            sys.stderr.write('Creating a new table in the database.\n')
            fields = [i[0] + ' ' + i[1] for i in fields]
            fields = ','.join(fields)

            self.cursor.execute(
                '''CREATE  TABLE ''' + tablename + ''' (''' + fields + ''')''')
            self.cursor.commit()
        return
