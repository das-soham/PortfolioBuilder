import sqlite3
import sys
import os
from dateutil.rrule import *
from dateutil.parser import *
from datetime import *
import copy
from TradeBook import TradeBook


class Portfolio():
    def __init__(self):
        self.portfolio = {}
        self.portfolio_table = 'Portfolio'
        self.positions = {}
        self.cursor = None
        self.connection = None
        return

    def build(self, trade_table, freshtrades: TradeBook, connector=None, db_path=None, db_name=None):
        '''
        The function builds the portfolio from trade_table
        :param trade_table: The excel sheet or Sqlite Table name from which to build the portfolio from
        :param connector: The SQLite3 connector
        :param db_path: The path to a pre-existing db
        :param db_name: The name to a pre-existing db
        :return: None
        '''
        update_build_flag = 0  # build =0, update =1
        try:
            if connector is None:  # no connector passed...
                if db_name is None:  # and no db given
                    raise ValueError  # no source to build portfolio from
                else:
                    self.connection = sqlite3.connect(os.path.join(db_path, db_name))
                    self.cursor = self.connection.cursor()  # db name and db path passed; build a connector & cursor
            else:
                self.cursor = connector.cursor()  # connector is passed, build a cursor

            update_build_flag = self.__table_exists(self.portfolio_table, [])
            if update_build_flag == 0:  # Build = 0
                res = self.cursor.execute(
                    '''select min(TradeDate) from ''' + trade_table).fetchall()  # query for the first trade date
            else:  # Update = 1

                res = self.cursor.execute(
                    '''select max(Date) from ''' + self.portfolio_table).fetchall()  # query for the last pf date
                # logic to populate self.portfolio dictionary for max(Date)
                res_pf_table = self.cursor.execute(
                    '''SELECT Ticker,Qty,Price from ''' + self.portfolio_table + ''' where Date =:date_time''',
                    {'date_time': res[0][0]}).fetchall()
                min_ftrade_dt = freshtrades.min('Date')
                if min_ftrade_dt is not None:
                    res = min((datetime.strptime(res[0][0], "%Y-%m-%d 00:00:00") + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"),min_ftrade_dt)
                else:
                    res = (datetime.strptime(res[0][0], "%Y-%m-%d 00:00:00") + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

                for row in res_pf_table:
                    self.positions[row[0]] = (row[1], row[2])

            if datetime.now().time() > time(15, 30, 00):
                date_range = list(rrule(DAILY, dtstart=parse(res), until=datetime.now().date(),
                                        byweekday=range(5)))  # build a list of weekdays
            else:
                date_range = list(
                    rrule(DAILY, dtstart=parse(res), until=datetime.now().date() - timedelta(days=1),
                          byweekday=range(5)))  # build a list of weekdays

            for i in range(len(date_range)):  # iterate through the list of dates

                if self.portfolio.get(date_range[i - 1].strftime(
                        "%Y-%m-%d 00:00:00")) is not None:  # i.e. data exists in the portfolio dictionary for the previous day
                    self.positions = copy.deepcopy(self.portfolio[date_range[i - 1].strftime("%Y-%m-%d 00:00:00")])

                res = self.cursor.execute('''SELECT * from ''' + trade_table + ''' where TradeDate =:date_time''',
                                          {'date_time': date_range[i].strftime("%Y-%m-%d 00:00:00")}).fetchall()
                for row in res:
                    if self.positions.get(row[1]) is None:  # new ticker, new position
                        self.positions[row[1]] = (row[3], row[4])  # add qty, and price to the dictionary
                    else:
                        if row[2] == 'Buy':  # old position, scaling up of trade
                            wtd_price = round(
                                (self.positions[row[1]][0] * self.positions[row[1]][1] + row[3] * row[4]) / (
                                        self.positions[row[1]][0] + row[3]), 2)  # weighted average pricing
                            self.positions[row[1]] = (self.positions[row[1]][0] + row[3], wtd_price)  # updated quantity
                        else:
                            self.positions[row[1]] = (self.positions[row[1]][0] - row[3],
                                                      self.positions[row[1]][1])  # update quantity, and price?
                self.portfolio[date_range[i].strftime("%Y-%m-%d 00:00:00")] = copy.deepcopy(self.positions)



        except ValueError:
            sys.stderr.write('NO CONNECTOR or DB NAME provided')
            return
        except sqlite3.OperationalError:
            sys.stderr.write('SQL error')
            return

        return

    def write_db(self, connection=None, db_path=None, db_name=None):
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
            self.__table_exists(self.portfolio_table, fields)

            for k in self.portfolio.keys():
                for pos in self.portfolio[k].keys():
                    self.cursor.execute('''INSERT INTO Portfolio ('Date','Ticker','Price','Qty') VALUES (?,?,?,?)''',
                                        (k, pos, self.portfolio[k][pos][1], self.portfolio[k][pos][0]))
            self.connection.commit()
            self.cursor.execute('''DELETE FROM ''' + self.portfolio_table + ''' where Qty = 0''')
            self.connection.commit()
            return
        except ValueError:
            sys.stderr.write('No prior portfolio loaded into memory')
            return

    def __table_exists(self, tablename, fields):
        row = self.cursor.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                                  {'type_name': 'table', 'table': tablename}).fetchall()
        if len(row) == 0:
            if len(fields) != 0:
                sys.stderr.write('No such table exists in the database.\n')
                sys.stderr.write('Creating a new table in the database.\n')
                fields = [i[0] + ' ' + i[1] for i in fields]
                fields = ','.join(fields)

                self.cursor.execute(
                    '''CREATE  TABLE ''' + tablename + ''' (''' + fields + ''')''')
                self.connection.commit()
            return 0
        else:
            return 1
