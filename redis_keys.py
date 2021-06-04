import redis
import json
from google.cloud import bigquery
import pandas_gbq
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:/Users/asus/Desktop/dev-mariner-310616-79bae9988bba.json'
client = bigquery.Client()

sql = """
SELECT DISTINCT
  pcoordinate_x,
  pcoordinate_y,
  pcode,
  porigin,
  scode,
  pmetadata_city,
  pmetadata_address,
  pmetadata_provider,
  pmetadata_accessType,
  pmetadata_capacity,
  pmetadata_categories,
  smetadata_outlets_outletTypeCode,
  smetadata_outlets_maxPower,
  smetadata_outlets_maxCurrent,
  smetadata_outlets_minCurrent,
  altitude
 FROM `dev-mariner-310616.e_charging.final_stations`
"""

df = pandas_gbq.read_gbq(sql, project_id='dev-mariner-310616')

dic = json.loads(df.to_json(orient="index"))

stations = dict()

for key in dic:
    pcode = dic[key]['pcode']
    del dic[key]['pcode']

    if not pcode in stations:
        stations[pcode] = []

    stations[pcode].append(dic[key])

client = redis.Redis(host='localhost', port=6379)

for sta in stations:
    client.set(sta, json.dumps(stations[sta]))

test = json.loads(client.get('DW-000027').decode('utf8'))
print(test)
