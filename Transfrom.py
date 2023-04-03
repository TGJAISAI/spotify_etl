import pandas as pd
import numpy as np
from Extract import getdata
import  datetime 


class trans(getdata):

    def __init__(self):
        super().__init__()
        self.data = self.extract()
        print(self.data)

    def data_quality(self):
        today = datetime.date.today()  # Get today's date
        yesterday = today - datetime.timedelta(days=1) 
        
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.current_data =  self.data[self.data['timestamp'] == str(yesterday)]
        print(self.current_data )
        if self.current_data.empty:
            print('No songs Extracted')
            return False

        #Enforcing Primary keys since we don't need duplicates
        if pd.Series(self.current_data['played_at']).is_unique:
            pass
        else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
         raise Exception("Primary Key Exception,Data Might Contain duplicates")
        
        if self.current_data.isnull().values.any():
            raise  Exception("Null values found")
        
        return self.data
        
    def transform(self):
        self.Transformed_df = self.data.groupby(['artist_name'], as_index= False)['song_name'].count()
        self.Transformed_df.rename(columns ={"song_name" : "count"}, inplace=True)
        return self.Transformed_df

    #     Transformed_df=self.data.groupby(['timestamp','artist_name'],as_index = False).count()
    #     Transformed_df.rename(columns ={'played_at':'count'}, inplace=True)

    # #Creating a Primary Key based on Timestamp and artist name
    #     Transformed_df["ID"] = Transformed_df['timestamp'].astype(str) +"-"+ Transformed_df["artist_name"]

        # return Transformed_df[['ID','timestamp','artist_name','count']]



if __name__ == '__main__':
    dg = trans()
    dg.data_quality()
    dg.transform()
  



