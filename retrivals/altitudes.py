from google.cloud import bigquery
import pandas_gbq
import os
import requests
import time
import pandas as pd
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:/Users/asus/Desktop/dev-mariner-310616-79bae9988bba.json'
client = bigquery.Client()

sql = """
    SELECT pcoordinate_x, pcoordinate_y FROM `dev-mariner-310616.e_charging.final_stations` GROUP BY pcoordinate_x, pcoordinate_y
"""

df = pandas_gbq.read_gbq(sql, project_id='dev-mariner-310616')

url = 'https://api.opentopodata.org/v1/etopo1?locations='  # 45.665711,11.7842632

for index, row in df.iterrows():
    coordinates = str(df.iloc[index]['pcoordinate_y']) + ',' + str(df.iloc[index]['pcoordinate_x'])
    full_url = url + coordinates

    response = requests.request("GET", full_url)
    data = response.json()
    df.loc[index, 'altitude'] = data['results'][0]['elevation']

    time.sleep(5)

pandas_gbq.to_gbq(df, destination_table='e_charging.altitudes', project_id='dev-mariner-310616')
