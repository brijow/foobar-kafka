import os, sys, re, gzip, datetime
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel, uuid
import pandas as pd

tablename = os.getenv("weather.table", "weatherreport")
twittertable  = os.getenv("twittertable.table", "twitterdata")

def saveTwitterDf(dfrecords, cassandrahost, keyspace):
    if isinstance(cassandrahost, list):
        cluster = Cluster(cassandrahost)
    else:
        cluster = Cluster([cassandrahost])
    
    session = cluster.connect(keyspace)
    
    counter = 0
    totalcount = 0
    
    cqlsentence = "INSERT INTO " + twittertable +" (tweet_date, location, tweet, classification) \
                   VALUES (?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['datetime'], val['location'], val['tweet'], val['classification']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter    
    rs = [session.execute(b, trace=True) for b in batches]
    
    print('Inserted ' + str(totalcount) + ' rows in total')


def saveWeatherreport(dfrecords, cassandrahost, keyspace):
    if isinstance(cassandrahost, list):
        cluster = Cluster(cassandrahost)
    else:
        cluster = Cluster([cassandrahost])
    
    session = cluster.connect(keyspace)
    
    counter = 0
    totalcount = 0
    
    cqlsentence = "INSERT INTO " + tablename +" (forecastdate, location, description, temp, feels_like, temp_min, temp_max, pressure, humidity, wind, sunrise, sunset) \
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert = session.prepare(cqlsentence)
    batches = []
    for idx, val in dfrecords.iterrows():
        batch.add(insert, (val['report_time'], val['location'], val['description'], 
                          val['temp'], val['feels_like'], val['temp_min'], val['temp_max'],
                          val['pressure'], val['humidity'], val['wind'], val['sunrise'], val['sunset']))
        counter += 1
        if counter >= 100:
            print('inserting ' + str(counter) + ' records')
            totalcount += counter
            counter = 0
            batches.append(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter    
    rs = [session.execute(b, trace=True) for b in batches]
    
    print('Inserted ' + str(totalcount) + ' rows in total')


def loadDF(targetfile, target, host, keyspace):
    if target == 'weather':
        colsnames=['description', 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'wind', 'sunrise', 'sunset', 'location', 'report_time']
        dfData = pd.read_csv(targetfile, header=None, parse_dates=True, names=colsnames)
        dfData['report_time'] = pd.to_datetime(dfData['report_time'])
        saveWeatherreport(dfData, host, keyspace)
    elif target == 'twitter':
        colsnames=['tweet', 'datetime', 'location', 'classification']
        dfData = pd.read_csv(targetfile, header=None, parse_dates=True, names=colsnames)
        dfData['datetime'] = pd.to_datetime(dfData['datetime'])
        saveTwitterDf(dfData, host, keyspace)

if __name__ == "__main__":
    keyspace = 'kafkapipeline'
    host = 'localhost'
    target = sys.argv[1]
    targetfile = sys.argv[2]
    loadDF(targetfile, target, host, keyspace)