import redis
import json

with open('stations.json', 'r') as stat:
    stations = json.load(stat)


client = redis.Redis(host='localhost', port=6379)

for sta in stations:
    client.set(sta, json.dumps(stations[sta]))

test = json.loads(client.get('DW-000027').decode('utf8'))
print(test)




