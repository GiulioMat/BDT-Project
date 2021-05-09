from google.cloud import storage
import os
import pandas as pd
import json
import datetime as datetime
import requests


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/francesca_padovani98/dev-mariner-310616-79bae9988bba.json'

client = storage.Client()
bucket = client.get_bucket('e_stations')

start_date = datetime.date(2019, 1, 1)
end_date = datetime.date(2019, 12, 31)
delta = datetime.timedelta(days=1)

while start_date <= end_date:
    date = start_date
    df_day = pd.DataFrame()
    
    for i in range(0,24):
        string_date = str(date) + "T" + datetime.time(i).strftime("%H") + "/" + str(date) + "T" + datetime.time(i).strftime("%H:59")

        url = "https://mobility.api.opendatahub.bz.it/v2/flat,node/EChargingPlug/*/" + string_date + "?where=pactive.eq.true&limit=-1"
        
        try:
            response = requests.request("GET", url)

            data = response.json()
        
            df = pd.json_normalize(data['data'])
            
            index = df[df['smetadata.outlets'].notnull()].index.tolist()

            df_add = pd.json_normalize(([d[0] for d in df['smetadata.outlets'].iloc[index]])).add_prefix('smetadata.outlets.')
            df_add['index'] = index

            df = pd.merge(df, df_add, how='left', left_index=True, right_on='index').reset_index().drop(columns=['level_0','index', 'smetadata.outlets'], axis=1)
            
            columnsDrop = ['tdescription', 'tname', 'ttype', 'tunit', 'mvalidtime', 'pavailable', 'pmetadata.accessInfo', 'pmetadata.reservable', 'pmetadata.paymentInfo', 'ptype', 'savailable', 'scoordinate.x', 'scoordinate.y', 'scoordinate.srid', 'stype', 'smetadata.outlets.hasFixedCable'] 
            df = df.drop(columns=columnsDrop, axis=1)
        
            df_day = df_day.append(df, ignore_index=True)
            
        except: 
            print(string_date)
            
    name = str(date) + '.csv'
    bucket.blob(name).upload_from_string(df_day.to_csv(), 'text/csv')
    
    start_date += delta