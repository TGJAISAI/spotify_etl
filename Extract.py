import pandas as pd
import requests
from datetime import  datetime
import datetime


class getdata:

    def __init__(self):
        self.USER_ID = "315yoc5kmgplgmb7y47qlshv7ltm"
        self.TOKEN =  "BQDja8uZBlzvs-SWP4BdatnjvXdvUygktsFzgmSJVpDxQn7ULVF96Jfe_A5g_9H4IYKRO8IpMJ7nnhiuqtO4Eg2xehwgkAqCZIAN-9Y7Vb3ceglaJ8ccnLMghQIdZ5_6sGf0HtcZP_5cxL6GbXglnThyWpAawNRb0sbUTMq2G9DLhqOHV-xvTyq-HCpEGWP6HHQj8Q1GnA"
    def extract(self):
        input_variables ={
            "Accept" : "application/json",
            "Content-Type" : "application/json",
            "Authorization" : "Bearer {token}".format(token= self.TOKEN )

        }

        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days = 1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
        


        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp), headers = input_variables)
        data = r.json()

        song_name = []
        artist_names = []
        played_at_list = []
        timestaps = []
        timestamps = []
        


        for song in data["items"]:
            song_name.append(song["track"]["name"])
            artist_names.append(song['track']["album"]["artists"][0]['name'])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])

        song_dict ={
            "song_name" : song_name,
            "artist_name" : artist_names,
            "played_at" : played_at_list,
            "timestamp" : timestamps
        }
        
        df = pd.DataFrame.from_dict(song_dict, orient='index')
        df = df.transpose()
        return df


if __name__ == '__main__':
    dg = getdata()
    dg.extract()





    

