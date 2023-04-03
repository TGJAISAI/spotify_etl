import psycopg2 
import requests
import json
from datetime import datetime
import datetime
import sqlite3
from Transfrom import trans
from Extract import  getdata
from sqlalchemy import create_engine

class load(trans):

    def __init__(self):
        super().__init__()
        self.Transformed_df = self.transform()
        self.full_data = self.data_quality()

    def database(self):
        url = "mysql+pymysql://root:@127.0.0.1:3306/my_spotify"
        engine = create_engine(url)
        print(engine)


        
        query_1 = """CREATE TABLE IF NOT EXISTS fav_artist(
                timestamp VARCHAR(200),
                ID VARCHAR(200),
                artist_name VARCHAR(200),
                count int)  """


        query_2 = """CREATE TABLE IF NOT EXISTS my_played_tracks(
            ID SERIAL PRIMARY KEY, 
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200))"""
        
        engine.execute(query_1)
        engine.execute(query_2)
        print("Opened database successfully")
        # print(self.full_data)
        

        try:
            self.full_data.to_sql("my_played_tracks",engine , index= False, if_exists= 'append' )
        except:
            print("data already exists in the database")

        try: 
            self.Transformed_df.to_sql(name = "fav_artist", con =  engine, index=False, if_exists='append')
        except:
            print("Data already exists in the database2")     

        # # close the cursor and connection
        # engine.close()

        
if __name__ == '__main__':
    dg = load()
    dg.database()

