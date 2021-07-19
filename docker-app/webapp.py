import flask
from flask import request, render_template
import json
import redis
from joblib import dump, load
from datetime import datetime
import pandas as pd
import numpy as np
import requests

app = flask.Flask(__name__)

# connect to redis
client = redis.Redis(host='redis', port=6379, decode_responses=True)

# load random forest model
randomforest = load('./model/rf.joblib')

# station states to be predicted
states = ['ACTIVE', 'AVAILABLE', 'FAULT', 'OCCUPIED', 'TEMPORARYUNAVAILABLE', 'UNKNOWN']

# file containing the input structure expected by the model
df_based = pd.read_csv('./model/expected_input.csv')

# retrieve forecast weather
def get_weather(date, hour, lat, long):
    try:
        url = 'http://api.weatherapi.com/v1/forecast.json?key=98978055e3eb4bbca9855750212205&q=' + str(lat) + ',' + str(
            long) + '&dt=' + str(date) + '&hour=' + str(hour)
        print(url)
        response = requests.request("GET", url)
        data = response.json()
        if data['forecast']['forecastday'] == []:
            return 'The received date is too far away (14 days limit) or in the past. Insert a date closer to the current one. ', -1
        return [data['forecast']['forecastday'][0]['hour'][0]['temp_c'],
                data['forecast']['forecastday'][0]['hour'][0]['precip_mm']]
    except:
        pass

# tourism seasonality given the date
def set_season(date):
    year = str(date.year)
    if (datetime.strptime(year + '-08-07', '%Y-%m-%d').date() < date < datetime.strptime(year + '-08-21', '%Y-%m-%d').date()) \
            or (datetime.strptime(year + '-12-18', '%Y-%m-%d').date() < date < datetime.strptime(year + '-01-09', '%Y-%m-%d').date()):
        return 4
    elif (datetime.strptime(year + '-07-05', '%Y-%m-%d').date() < date < datetime.strptime(year + '-08-07', '%Y-%m-%d').date()) \
            or (datetime.strptime(year + '-01-29', '%Y-%m-%d').date() < date < datetime.strptime(year + '-03-12', '%Y-%m-%d').date()) \
            or (datetime.strptime(year + '-08-21', '%Y-%m-%d').date() < date < datetime.strptime(year + '-08-28', '%Y-%m-%d').date()):
        return 3
    elif (datetime.strptime(year + '-03-26', '%Y-%m-%d').date() < date < datetime.strptime(year + '-06-26', '%Y-%m-%d').date()) \
            or (datetime.strptime(year + '-09-26', '%Y-%m-%d').date() < date < datetime.strptime(year + '-11-07', '%Y-%m-%d').date()):
        return 1
    else:
        return 2

# rename columns of output table, for clarity
def rename_columns(df):
    df.rename(columns={'pcode': 'Station ID', 'pcoordinate_x': 'Longitude', 'pcoordinate_y': 'Latitude', 'porigin': 'Source',
                       'pmetadata_city': 'City', 'pmetadata_address': 'Address', 'pmetadata_provider': 'Provider',
                       'pmetadata_accessType': 'Access type', 'pmetadata_capacity': 'Number of plugs', 'pmetadata_categories': 'Category',
                       'altitude': 'Altitude', 'scode': 'Plug ID', 'smetadata_outlets_outletTypeCode': 'Connector type',
                       'smetadata_outlets_maxPower': 'Max power', 'smetadata_outlets_maxCurrent': 'Max current',
                       'smetadata_outlets_minCurrent': 'Min current', 'mvalue': 'Plug state'}, inplace=True)
    return df


# return homepage with the links to the functions
@app.route('/', methods=['GET'])
def home():
    return render_template('home.html')


# return table with all the stations and their specifics
@app.route('/stations/all', methods=['GET'])
def app_all():
    df = pd.DataFrame()

    for station_id in client.keys():
        df_temp = pd.DataFrame(json.loads(client.get(station_id)))
        df_temp['pcode'] = str(station_id)
        df = df.append(df_temp)

    # show specifics only of the stations, not of every single plug
    df = df.drop(['scode', 'smetadata_outlets_outletTypeCode', 'smetadata_outlets_maxPower',
                  'smetadata_outlets_maxCurrent', 'smetadata_outlets_minCurrent'], axis=1).drop_duplicates()

    # put the column scode as the first column (just to visualize better)
    cols = list(df.columns)
    cols = [cols[-1]] + cols[:-1]
    df = df[cols].reset_index(drop=True)

    return render_template("totable.html", table=rename_columns(df).to_html())


# GET: return form where to insert the id of the station
# POST: return table with the specifics of the station selected and its plugs
@app.route('/stations/id', methods=['GET', 'POST'])
def app_id():
    if request.method == 'POST':

        station_id = request.form['id']
        data = client.get(station_id)

        if data is None:
            return "Error: The id provided is not present in the database. <a href=/stations/id> style=color:blue;>Go back</a>"

        # get data of the requested station
        df = pd.DataFrame(json.loads(data))
        df['pcode'] = str(station_id)

        # put the column scode as the first column (just to visualize better)
        cols = list(df.columns)
        cols = [cols[-1]] + cols[:-1]
        df = df[cols]

        return render_template("totable.html", table=rename_columns(df).to_html())

    else:
        return render_template('stationsid.html')


# GET: return form where to insert the id of the station and the date of the prediction (divided in year, month, day, hour)
# POST: return table with the prediction of the plugs obtained
@app.route('/predict', methods=['GET', 'POST'])
def app_prediction():
    if request.method == 'POST':

        # get id of station and date inserted
        station_id = request.form['id']
        year = request.form['year']
        month = request.form['month']
        day = request.form['day']
        hour = request.form['hour']

        check_empty = {'id': station_id,
                       'year': year,
                       'month': month,
                       'day': day,
                       'hour': hour
                       }

        for key, value in check_empty.items():
            if value == '':
                return "Error: No {} field provided. Please specify an {}. <a href=/predict style=color:blue;>Go back</a>".format(key, key)

        # compose date
        date = "{}-{}-{}T{}".format(year, month, day, hour)
        date = datetime.strptime(date, '%Y-%m-%dT%H')
        # get data of the station on redis
        data = client.get(station_id)

        if data is None:
            return "Error: The id provided is not present in the database. <a href=/predict style=color:blue;>Go back</a>"

        data = json.loads(data)
        # preprocess data and add weather forecast and seasonality
        df_station = pd.DataFrame(data)
        df_station['month'] = date.month
        df_station['day'] = date.day
        df_station['hour'] = date.hour
        temp, prec = get_weather(date.date(), date.hour, data[0]['pcoordinate_y'], data[0]['pcoordinate_x'])

        if prec == -1:
            return temp + "<a href=/predict style=color:blue;>Go back</a>"

        df_station['mvalue_t'], df_station['mvalue_p'] = temp, prec
        df_station['season'] = set_season(date.date())

        codes = df_station['scode']
        df_query = pd.get_dummies(df_station, dtype='int64')

        # make data have same format as expected input
        col_diff_add = df_based.columns.difference(df_query.columns)
        col_diff_remove = df_query.columns.difference(df_based.columns)

        new_add = col_diff_add.tolist()
        new_remove = col_diff_remove.tolist()
        for col in new_add:
            df_query[col] = 0
        df_query = df_query.drop(new_remove, axis=1)

        df_query = df_query[df_based.columns]

        # prediction stage
        df = pd.DataFrame(columns=['pcode', 'scode', 'Plug state', 'Station state', 'Prediction date'])

        for index, row in df_query.iterrows():
            probs = randomforest.predict_proba(np.array(row).reshape(1, -1))
            probs_np = np.array(probs).reshape(7, 2)
            mvalue_pred = np.argmax(probs_np[0], axis=0)
            state_pred = np.argmax(probs_np[1:7, 1], axis=0)

            plug_pred = {'pcode': station_id, 'scode': codes[index],
                         'Plug state': 'Available' if mvalue_pred == 1 else 'Not available',
                         'Station state': states[state_pred], 'Prediction date': date}
            df = df.append(plug_pred, ignore_index=True)

        return render_template("totable.html", table=rename_columns(df).to_html())

    else:
        # get today date to set as default in the form
        today = datetime.today()
        return render_template('predictions.html', year=today.year, month=today.month, day=today.day, hour=today.hour)


if __name__ == "__main__":
    app.run()
