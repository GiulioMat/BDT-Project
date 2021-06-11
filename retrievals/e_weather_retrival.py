from google.cloud import storage
import os
import pandas as pd
import json
import datetime as datetime
import requests


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/francesca_padovani98/dev-mariner-310616-79bae9988bba.json'

client = storage.Client()
bucket = client.get_bucket('e_weather')

start_date = datetime.date(2019, 1, 1)
end_date = datetime.date(2019, 12, 31)
delta = datetime.timedelta(days=1)

rel = ['air-temperature', 'precipitation']
codes = ['78305MS', '11400MS']

while start_date <= end_date:
    date = start_date
    df_day = pd.DataFrame()
    
    for i in range(0,24):
        
        string_date = str(date) + "T" + datetime.time(i).strftime("%H:00") + "/" + str(date) + "T" + datetime.time(i).strftime("%H:02")

        url = "https://mobility.api.opendatahub.bz.it/v2/flat,node/MeteoStation/*/" + string_date + "?where=sactive.eq.true&limit=-1"
        
        try:
            response = requests.request("GET", url)

            data = response.json()
        
            df = pd.json_normalize(data['data'])
            
            df = df.loc[df['scode'].isin(codes)]
            df = df.loc[df['tname'].isin(rel)]
            
            columnsDrop = ['tdescription', 'sorigin', 'tunit', 'mtransactiontime', 'savailable', 'sname', 'stype', 'smetadata.name_de', 'smetadata.name_en', 'smetadata.name_it'] 
            df = df.drop(columns=columnsDrop, axis=1)
        
            df_day = df_day.append(df, ignore_index=True)
            
        except: 
            print(string_date)
            
    name = str(date) + '.csv'
    bucket.blob(name).upload_from_string(df_day.to_csv(index=False), 'text/csv')
    
    start_date += delta