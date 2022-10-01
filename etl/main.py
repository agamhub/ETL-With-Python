import datetime
import requests
import json
import psycopg2
import sys
sys.path.insert(0,'/ETL With Python/src')
from config import config
import DatabaseInsert as db
from pprint import pprint
import pandas as pd
from datetime import timedelta
import time

class etl:

    l = "https://data.covid19.go.id/public/api/prov.json"
    url = []
    
    def url(self,web):
        self.url = []
        self.url.append(web) 
        
        return self.url

    def api_req(self,base_url):
        try:
            self.req = requests.get(url = base_url)
            if self.req.status_code == 200:
                self.data = self.req.json()
            else:
                self.req.raise_for_status()
        except(Exception,requests.HTTPError) as e :
            print(e)

    def loop(self):
        for i in self.url(self.l):
            self.api_req(i)

    def extraction_prov_json(self):
        df = pd.DataFrame(self.data)
        df_last_date = df['last_date']
        df_list_data = pd.json_normalize(df['list_data']).rename(columns=({'lokasi.lon':'lokasi_lon','lokasi.lat':'lokasi_lat','penambahan.positif':
                                                                                        'penambahan_positif','penambahan.sembuh':'penambahan_sembuh',
                                                                                        'penambahan.meninggal':'penambahan_meninggal'
                                                                                        }))
        df_key = df_list_data[['key','doc_count','jumlah_kasus','jumlah_meninggal','jumlah_dirawat']]                                                                                
        df_kel = pd.DataFrame.from_records(df_list_data['jenis_kelamin']).to_json(orient='records')
        df_umur = pd.DataFrame.from_records(df_list_data['kelompok_umur']).to_json(orient='records')
        df_json_kel = pd.json_normalize(json.loads(df_kel))
        df_json_umur = pd.json_normalize(json.loads(df_umur))
        df_json_kel.rename(columns=({'0.key':'jenis_l','0.doc_count':'count_l','1.key':'jenis_p','1.doc_count':'count_p'}),inplace=True)
        df_json_umur.rename(columns=({'0.key':'umur_1','0.doc_count':'umur_count_1','0.usia.value':'usia_val_1',
                                                                                '1.key':'umur_2','1.doc_count':'umur_count_2','1.usia.value':'usia_val_2',
                                                                                '2.key':'umur_3','2.doc_count':'umur_count_3','2.usia.value':'usia_val_3',
                                                                                '3.key':'umur_4','3.doc_count':'umur_count_4','3.usia.value':'usia_val_4',
                                                                                '4.key':'umur_5','4.doc_count':'umur_count_5','4.usia.value':'usia_val_5'}),inplace=True)
        df_con = pd.concat([df_list_data,df_json_kel,df_json_umur],axis=1)
        df_con['logname'] = 'extraction_prov_json'
        df_final = df_con[['key','doc_count','jumlah_kasus','jumlah_sembuh','jumlah_meninggal','jumlah_dirawat','jenis_l','count_l','jenis_p','count_p'
                    ,'umur_1','umur_count_1','usia_val_1','umur_2','umur_count_2','usia_val_2','umur_3','umur_count_3','usia_val_3'
                    ,'umur_4','umur_count_4','usia_val_4','umur_5','umur_count_5','usia_val_5','lokasi_lon','lokasi_lat','penambahan_positif','penambahan_sembuh','penambahan_meninggal'
                ]].astype("string")
        df_final['attu_timestamp'] = df_last_date + " " + str(pd.Timestamp.now().time())
        df_concat = pd.concat([df_con[['logname']][:1],df_final[['attu_timestamp']][:1]],axis=1)
        df_col = df_final.columns  
        db.Database.__init__(self)
        db.Database.dynamic_col(self,df_col) 
        db.Database.join_list(self,df_final)
        db.Database.tuple(self,df_final)
        for i in df_concat.values:
            if db.Database.update_log(self,i,'extraction_prov_json') == "No Updated":
                print('The latest data still same')
            else:
                db.Database.execute_many(self,'fp_db.covid_all')

if __name__ == "__main__":
    pop = etl()
    pop.loop()
    pop.extraction_prov_json()
    