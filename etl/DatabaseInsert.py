import psycopg2
import sys
sys.path.insert(0,'/ETL With Python/src')
from config import config
import numpy as np

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

    def join_list(self,cols):
        self.cols = ','.join(list(cols.columns))
        self.val = ','.join(list(self.lst))

    def tuple(self,df):
        self.tuples = [tuple(x) for x in df.to_numpy()]    

    def update_log(self,val,log):
        sql_exists = "SELECT EXISTS(SELECT 'log exists' FROM fp_db.logging WHERE logging_name = %s AND CAST(attu_timestamp AS DATE) = CAST(%s AS DATE))"
        sql_insert = "INSERT INTO fp_db.logging(logging_name,attu_timestamp) VALUES(%s,%s)"
        sql_update = "UPDATE fp_db.logging SET (logging_name,attu_timestamp) = (%s,%s) WHERE logging_name = '"+ log +"'"
        try:
            self.cur = self.connection.cursor()
            self.cur.execute(sql_exists,val)
            if self.cur.fetchone()[0]:
                return 'No Updated'
            else:    
                self.cur.execute(sql_update,val)
                self.cur.execute(sql_insert,val)
            self.connection.commit()
        except(Exception,psycopg2.DatabaseError) as e:
            print("Error : %s" % e)
            self.connection.rollback()
            self.cur.close()
            return 1
        print("execute update_log() done")
        self.cur.close()
        
    def execute_many(self,table):
        query  = "INSERT INTO %s(%s) VALUES(%s)" % (table,self.cols,self.val)
        try:
            self.cur = self.connection.cursor()
            self.cur.executemany(query, self.tuples)
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cur.close()
            return 1
        print("execute_many() done")
        self.cur.close()