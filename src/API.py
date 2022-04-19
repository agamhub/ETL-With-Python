import json
from logging import exception
from time import time
import requests
import psycopg2
from config import config
import pandas as pd
import numpy as np

# variables
stime = time()
food_list = ['pizza','burger','pie','rice','noodle']
json_list = []

base_url = "https://api.punkapi.com/v2/beers?"

class etl:
    def __init__(self):
        try:
            self.params = config()
            self.connection = psycopg2.connect(**self.params)
            self.connection.autocommit = True
            self.cur = self.connection.cursor()
        except(Exception, psycopg2.DatabaseError) as e:
            print(e)

    def insert(self,id,name):
        insert_command = "INSERT INTO fp_db.users(userid,username) VALUES('" + id +"','"+ name +"')"
        self.cur.execute(insert_command)

    def execute_many(self, df, table):
        """
        Using cursor.executemany() to insert the dataframe
        """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
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

    def params(x):
        return dict(food = x)

    for i in food_list:
        try:
            req = requests.get(url = base_url, params = params(i))
            data = req.json()
            for l in data:
                id = l['id']
                name = l['name']
                volume_value = l['volume']['value']
                volume_unit = l['volume']['unit']
                method_mash = l['method']['mash_temp'][0]['temp']['value']
                method_fermentation = l['method']['fermentation']['temp']['value']
                food_pair = l['food_pairing']

                x = {
                        "id": id,
                        "name": name,
                        "volume_value": volume_value,
                        "volume_unit": volume_unit,
                        "method_mash": method_mash,
                        "method_fermentation": method_fermentation,
                        "total_of_method_value": (method_mash+method_fermentation),
                        "food pair": food_pair,
                        "email": i
                }
                json_list.append(x)

        except requests.exceptions.HTTPError as e:
            print(e)

df = pd.DataFrame(json_list)
df1 = pd.DataFrame([pd.Series(x) for x in df['food pair']])
df1.columns = [f"foor_pair_{x+1}" for x in df1.columns]
df = pd.concat([df, df1], axis=1)
df2 = df['id'].apply(lambda x: '{0:0>10}'.format(x))
df = pd.concat([df2,df['name'],df['email'],df['total_of_method_value']], axis=1)
df = df.rename(columns={'id':'userid','name':'username'})
df['dupe'] = df.duplicated('userid')
df_g = df.groupby(['userid']).sum('total_of_method_value').reset_index()
df_s = df.sort_values(by=['userid'])
df_s['count'] = df_s['userid'].map(df['userid'].value_counts())
df_tail = df_s.head(10)
print(df)
with pd.ExcelWriter("food.xlsx") as w:
    df.to_excel(w, sheet_name="food")

if __name__ == "__main__":
    etl_con = etl() 
    etl_con.execute_many(df,'fp_db.users')
 
print(time()-stime)