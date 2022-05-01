import psycopg2
import sys
sys.path.insert(0,'/ETL With Python/src')
from config import config

class Database:
    def __init__(self):
        try:
            self.params = config()
            self.connection = psycopg2.connect(**self.params)
            self.connection.autocommit = True
            self.cur = self.connection.cursor()
        except(Exception, psycopg2.DatabaseError) as e:
            print(e)

    def dynamic_col(self,t):
        self.lst = []
        for i in range(len(t)):
            self.lst.append('%s')  

    def execute_many(self,df,table):
        """
        Using cursor.executemany() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        val = ','.join(list(self.lst))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES(%s)" % (table, cols,val)
        try:
            self.cur.executemany(query, tuples)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cur.close()
            return 1
        print("execute_many() done")
        self.cur.close()