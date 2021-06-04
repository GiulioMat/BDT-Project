import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import random as rnd
from google.cloud import bigquery
import pandas_gbq
import os
import requests
import time
import pandas as pd
import json
from joblib import dump, load

print("Libraries loaded")

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/francesca_padovani98/dev-mariner-310616-79bae9988bba.json'
client = bigquery.Client()

sql = """
SELECT
  pcoordinate_x,
  pcoordinate_y,
  mvalue,
  pcode,
  porigin,
  scode,
  pmetadata_state,
  pmetadata_provider,
  pmetadata_accessType,
  pmetadata_capacity,
  pmetadata_categories,
  smetadata_outlets_outletTypeCode,
  smetadata_outlets_maxPower,
  smetadata_outlets_maxCurrent,
  smetadata_outlets_minCurrent,
  mvalue_p,
  mvalue_t,
  season,
  altitude,
  month,
  day, 
  hour
 FROM `dev-mariner-310616.e_charging.final_stations`
"""

df = pandas_gbq.read_gbq(sql, project_id='dev-mariner-310616')

print("Query executed")

df.month = df.month.astype('int64')
df.hour = df.hour.astype('int64')
df.day = df.day.astype('int64')

df = df.drop(['pcode', 'scode'], axis=1)

df = pd.get_dummies(df, dtype='int64')
df = df.drop(["pmetadata_categories_['']"], axis=1)
df = df.dropna(axis=1)

labels = np.array(df[['mvalue', 'pmetadata_state_ACTIVE', 'pmetadata_state_AVAILABLE', 'pmetadata_state_FAULT',
                      'pmetadata_state_OCCUPIED', 'pmetadata_state_TEMPORARYUNAVAILABLE', 'pmetadata_state_UNKNOWN']])
# Remove the labels from the features
# axis 1 refers to the columns
features = df.drop(['mvalue', 'pmetadata_state_ACTIVE', 'pmetadata_state_AVAILABLE', 'pmetadata_state_FAULT',
                    'pmetadata_state_OCCUPIED', 'pmetadata_state_TEMPORARYUNAVAILABLE', 'pmetadata_state_UNKNOWN'],
                   axis=1)
# Saving feature names for later use
# feature_list = list(features.columns)
# Convert to numpy array
features = np.array(features)

# Split the data into training and testing sets
train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size=0.25,
                                                                            random_state=42)

print("Starting training")

# Instantiate model with 1000 decision trees
rf = RandomForestClassifier(n_estimators=100, random_state=42)
# Train the model on training data
rf.fit(train_features, train_labels)

predictions = rf.predict(test_features)
# Calculate the absolute errors
errors = abs(predictions - test_labels)
# Print out the mean absolute error (mae)
print('Mean Absolute Error:', round(np.mean(errors), 2), 'degrees.')


dump(rf, './rf.joblib')
