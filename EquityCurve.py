import sys
from datetime import *
import sqlite3
import os


class Equity:
    def __init__(self, connector, cursor):
        self.equity = {}
        self.infusion = {}
        self.NAV = {}
        self.Units = {}
        self.connector = connector
        self.connection = None
        self.cursor = cursor
        self.tablename = 'Equity'
        self.update_build_flag = self.__table_exists(self.tablename, [])  # ??
        self.eq_last = self.cursor.execute('''select max(Date) from ''' + self.tablename).fetchall()[0][0]

    def alter_table(self,pf_from):
        #pf_from : date from which portfolio is generated = Portfolio.update_from

        if pf_from <= self.eq_last:
            self.cursor.execute('''DELETE FROM EQUITY WHERE DATE> =:date_time''', {'date_time': pf_from})

    def net_infusion(self, db_path, db_name):
        # net amount of infusion: deposits for today less withdrawals of total
        try:
            if self.connector is None:  # no connector passed...
                if db_name is None:  # and no db given
                    raise ValueError  # no source to build portfolio from
                else:
                    self.connection = sqlite3.connect(os.path.join(db_path, db_name))
                    self.cursor = self.connection.cursor()  # db name and db path passed; build a connector & cursor
            else:
                self.cursor = self.connector.cursor()  # connector is passed, build a cursor
        except ValueError:
            sys.stderr.write("\nNo db name or valid connector passed to the routine")
            return
        except sqlite3.OperationalError:
            sys.stderr.write("\nCouldn't connect to the database")
            return

        if self.update_build_flag:  # update =1; build = 0
            # get max date in the equity curve
            # query for infusions after the max date

            res = self.cursor.execute(
                '''select TradeDate,sum(case when Action = 'Buy' then Qty* Price else -1*Qty*Price end) as 'Infusion' 
                from TradeBook group by TradeDate having TradeDate > :date_time''',
                {'date_time': self.eq_last}).fetchall()  # query for net infusion numbers after a particular date
        else:
            res = self.cursor.execute(
                '''select TradeDate,sum(case when Action = 'Buy' then Qty* Price else -1*Qty*Price end) as 'Infusion' 
                from TradeBook group by TradeDate''').fetchall()

        for entry in res:
            self.infusion[entry[0]] = entry[1]

    def NAV_Qty(self):
        # Build the portfolio value on a daywise basis
        if self.update_build_flag:
            # get the maximum date available in EquityCurve table
            res = self.cursor.execute('''SELECT Date, SUM(Value) FROM (SELECT Portfolio.Date as Date, 
            Portfolio.Ticker, (Portfolio.Qty * Quotes.Close)as Value FROM Portfolio, Quotes WHERE Portfolio.Ticker = 
            Quotes.Ticker and Portfolio.Date = Quotes.Date) GROUP BY Date HAVING DATE >:date_time''',
                                      {'date_time': self.eq_last}).fetchall()
        else:
            res = self.cursor.execute('''SELECT Date, SUM(Value) FROM (SELECT Portfolio.Date as Date, 
            Portfolio.Ticker,(Portfolio.Qty * Quotes.Close)as Value FROM Portfolio, Quotes WHERE Portfolio.Ticker = 
            Quotes.Ticker and Portfolio.Date = Quotes.Date) GROUP BY Date''').fetchall()

        # Portfolio Value
        for row in res:
            self.equity[row[0]] = row[1]
        first_date = self.cursor.execute('''select min(TradeDate) from TradeBook''').fetchall()[0][0]
        res = self.cursor.execute('''select NAV, Units from Equity where Date in (select max(Date) from 
        Equity)''').fetchall()
        beg_NAV = res[0][0]
        beg_units = res[0][1]

        # NAV/Qty calculation
        for k in self.equity.keys():
            if k == first_date:
                # start the series with these numbers
                beg_NAV = 10
                beg_units = 0

            end_units = (self.infusion.get(k, 0) / beg_NAV) + beg_units
            self.NAV[k] = self.equity[k] / end_units
            self.Units[k] = end_units
            beg_NAV = self.NAV[k]  # NAV for next date
            beg_units = self.Units[k]  # Qty for next date

    def equity_curve(self) -> int:
        fields = [('Date', 'text'), ('PortfolioValue', 'real'), ('Infusion', 'real'), ('NAV', 'real'),
                  ('Units', 'real')]
        self.__table_exists(self.tablename, fields=fields)
        try:
            for k in self.equity.keys():
                self.cursor.execute('''INSERT INTO ''' + self.tablename + '''(Date,PortfolioValue,Infusion,NAV,Units) 
                VALUES(?,?,?,?,?)''', (
                    datetime.strptime(k, "%Y-%m-%d 00:00:00"), self.equity[k], self.infusion.get(k, 0),
                    self.NAV.get(k, 0),
                    self.Units.get(k, 0)))
            self.connector.commit()
            return 0
        except ValueError:
            sys.stderr.write("Error writing records in Equity table")
            return -1

    def __ondate__(self, on_date) -> dict:
        if isinstance(on_date, datetime):
            on_date = [on_date]
        on_date = ','.join(["'{}'".format(d) for d in on_date])
        status = self.__table_exists(self.tablename, [])
        if status == 1:
            res = self.cursor.execute(
                '''SELECT * FROM ''' + self.tablename + ''' where Date in ({})'''.format(on_date)).fetchall()
        portfolio_dict = {}
        for row in res:
            portfolio_dict[datetime.strptime(row[0], "%Y-%m-%d 00:00:00")] = row[1]

        return portfolio_dict

    def load(self):
        status = self.__table_exists(self.tablename, [])
        if status == 1:
            res = self.cursor.execute('''SELECT * FROM ''' + self.tablename).fetchall()
        for row in res:
            self.equity[datetime.strptime(row[0], "%Y-%m-%d 00:00:00")] = row[1]

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
                self.connector.commit()
            return 0
        else:
            return 1
