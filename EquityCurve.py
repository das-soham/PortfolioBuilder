import sys
from datetime import *


class Equity:
    def __init__(self,connector, cursor):
        self.equity = {}
        self.connector = connector
        self.cursor = cursor
        self.tablename = 'Equity'

    def build(self) -> int:
        res = self.cursor.execute('''SELECT Date, SUM(Value) FROM (SELECT Portfolio.Date as Date, Portfolio.Ticker,
        (Portfolio.Qty * Quotes.Close)as Value FROM Portfolio, Quotes WHERE Portfolio.Ticker = Quotes.Ticker and 
        Portfolio.Date = Quotes.Date) GROUP BY Date''').fetchall()

        for row in res:
            self.equity[row[0]] = row[1]

        fields = [('Date', 'text'), ('PortfolioValue', 'real')]
        self.__table_exists(self.tablename, fields=fields)
        try:
            for k in self.equity.keys():
                self.cursor.execute('''INSERT INTO ''' + self.tablename + '''(Date,PortfolioValue) VALUES(?,?)''',
                                    (datetime.strptime(k, "%Y-%m-%d 00:00:00"), self.equity[k]))
            self.connector.commit()
            return 0
        except ValueError:
            sys.stderr.write("Error writing records in Equity table")
            return -1

    def __ondate__(self, on_date)-> dict:
        if isinstance(on_date, datetime):
            on_date = [on_date]
        on_date = ','.join(["'{}'".format(d) for d in on_date])
        status = self.__table_exists(self.tablename,[] )
        if status == 1:
            res = self.cursor.execute('''SELECT * FROM ''' + self.tablename + ''' where Date in ({})'''.format(on_date)).fetchall()
        portfolio_dict = {}
        for row in res:
            portfolio_dict[datetime.strptime(row[0], "%Y-%m-%d 00:00:00")] = row[1]

        return portfolio_dict

    def load(self):
        status = self.__table_exists(self.tablename,[] )
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
