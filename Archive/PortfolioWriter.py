import sqlite3
import os
import sys

class PortfolioWriter():
    def __init__(self, path, name):
        #check if the database exists or not at the first place
        try:
            connexion = sqlite3.connect(os.path.join(path, name))
            self.conn = connexion
        except sqlite3.OperationalError:
            self.conn = None
            return

        #check if the table Portfolio exists in the database or not
        cur = self.conn.cursor()
        row = cur.execute("SELECT name FROM sqlite_master where type =:type_name and name =:table ",
                          {'type_name': 'table', 'table': 'Portfolio'}).fetchall()
        if len(row) == 0:
            sys.stderr.write('No such table exists in the database.\n')
            sys.stderr.write('Creating a new table in the database.\n')
            cur.execute(
                '''CREATE  TABLE ''' + 'Portfolio' + ''' (Date text, Ticker text, Price real, Qty integer)''')
            self.conn.commit()
        else:
            pass

    def WritePortfolio(self,Portfolio_Dict):
        cur = self.conn.cursor()
        for k in Portfolio_Dict.keys():
            for pos in Portfolio_Dict[k].keys():
                cur.execute('''INSERT INTO Portfolio ('Date','Ticker','Price','Qty') VALUES (?,?,?,?)''',(k, pos,Portfolio_Dict[k][pos][1],Portfolio_Dict[k][pos][0]))
        self.conn.commit()
        cur.execute('''DELETE FROM Portfolio where Qty = 0''')
        self.conn.commit()