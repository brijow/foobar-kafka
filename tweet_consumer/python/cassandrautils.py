import os, sys, re, gzip, datetime
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel, uuid

tablename = os.getenv("weather.table", "weatherreport")

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
            batches.append(batches)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if counter != 0:
        batches.append(batch)
        totalcount += counter    
    rs = [session.execute(b, trace=True) for b in batches]
    
    print('Inserted ' + str(totalcount) + ' rows in total')
