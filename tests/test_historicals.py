import sqlite3
import unittest

from Archive import historicals


class MyTest(unittest.TestCase):
    def test_ConnectionError(self):
        stat = historicals.connect_db('Portfolio.db', 'C:/')
        self.assertEqual(stat[1],'Error')
    def test_ConnectWorking(self):
        stat = historicals.connect_db('Portfolio.db', 'K:/')
        self.assertEqual(stat[1],None)
    def test_DBEmpty(self):
        #create an empty database from scratch, connect and check if the table is working
        con = sqlite3.connect(":memory:")
        cur = con.cursor()
        stat = historicals.DB_Empty(con)
        self.assertEqual(stat,True)
    def test_TableExists(self):
        con = sqlite3.connect(":memory:")
        cur = con.cursor()
        stat = historicals.Table_Exists(con, 'Test')
        self.assertEqual(stat, False)
        conn.close()
if __name__=='__main__':
    unittest.main()