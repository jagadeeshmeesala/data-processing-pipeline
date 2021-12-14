#####################################################
## PROGRAM TO IMPLEMENT KINESIS PRODUCER THAT FETCHES WEATHER INFORMATION
## AND STREAMS THE DATA INTO KINESIS STREAM
####################################################

# necessary imports
import boto3
import datetime as dt
import pandas as pd
import time
from pandas.core.tools import numeric
import requests
import json
import numpy as np
import math


# function to create a client with aws for a specific service and region
def create_client(service, region):
    return boto3.client(
        service, 
        region_name=region,
        aws_access_key_id='AKIAWY6SRLTLTWMYK3PL',
        aws_secret_access_key='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'
        # aws_session_token=SESSION_TOKEN
        )

# function for generating new runtime to be used for timefield in ES
def get_date():

    today = str(dt.datetime.today()) # get today as a string
    year = today[:4]
    month = today[5:7]
    day = today[8:10]

    hour = today[11:13]
    minutes = today[14:16]
    seconds = today[17:19]

    # return a date string in the correct format for ES
    return "%s/%s/%s %s:%s:%s" % (year, month, day, hour, minutes, seconds)

# function for generating new runtime to be used for timefield in ES
def format_date(date):

    date = str(date) # get today as a string
    year = date[:4]
    month = date[5:7]
    day = date[8:10]

    # return a date string in the correct format for ES
    return "%s/%s/%s" % (year, month, day)


def find_station_name(record, stations_info):
     for _,station in stations_info.iterrows():
            if station['id'] == record['station']:
                return station['name']

# function to transform the data
def transform_data(data, stations_info):
    
    # dates = data['cdatetime'] # get the datetime field
    data = data.replace(np.nan, 0)
    transformed = []

    # loop over all records
    for _, record in data.iterrows():
        item = {}
        # find station Id
        station_name= find_station_name(record, stations_info)
       
        # station = stations_info.loc[stations_info['name'] == record["station"]]
        # print(station)
        # print('*** station id', station['name'])
        item['stationId'] = record['station']
        item['stationName'] = station_name
        if record['datatype'] == "PRCP":
            item['precipitation'] = record['value']
            item['snow'] = 0.0
            item['minTemp'] = 0.0
            item['maxTemp'] = 0.0

        if record["datatype"] == "SNOW":
            item['precipitation'] = 0.0
            item['snow'] = record['value']
            item['minTemp'] = 0.0
            item['maxTemp'] = 0.0
        if record["datatype"] == "TMIN":
            item['precipitation'] = 0.0
            item['snow'] = 0.0
            item['minTemp'] = record['value']
            item['maxTemp'] = 0.0
        if record["datatype"] == "TMAX":
            item['precipitation'] = 0.0
            item['snow'] = 0.0
            item['minTemp'] = 0.0
            item['maxTemp'] = record['value']

        item['observationDate'] = format_date(record['date']) # format as YYYY-MM-DD
        item['insertedTimeStamp'] = get_date() # current timestamp
        print('*** item**', item)
        transformed.append(item)
    
    # return the dataframe
    return pd.DataFrame(transformed)


# fetches weather information for MD stations for month of October for GHCND data
def fetch_weather_data():

    datasetid = 'GHCND'
    startdate = '2021-10-01'
    enddate = '2021-10-31'
    locationid= 'FIPS:24' # maryland
    datatypeid = 'PRCP,SNOW,TEMP,TMAX,TMIN'
    limit = 1000 # api restricts the data to 1000 rows for every call

    offset = 0

    baseUrl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={datasetid}&startdate={startdate}&enddate={enddate}&locationid={locationid}&includemetadata=true&units=metric&datatypeid={datatypeid}&limit={limit}&offset={offset}"

    headers = {
            'token': 'YmWIsqbWVOByimkultmIWeLGAztzSjCa'
        }

    url = baseUrl.format(datasetid=datasetid, startdate = startdate, enddate = enddate, locationid = locationid, datatypeid = datatypeid, limit = limit, offset = offset)
    response = requests.request("GET", url, headers=headers)

    results = json.loads(response.text)

    totalCount = results["metadata"]["resultset"]["count"]

    dataFrame = pd.DataFrame(results["results"])

    pagination = math.floor(totalCount/limit + 1)

    # api limits the result set to 1000
    # pagination to fetch the total count
    for loop in range(1, pagination):
        offset = 0
        offset = offset+limit*loop + 1
        url = baseUrl.format(datasetid=datasetid, startdate = startdate, enddate = enddate, locationid = locationid, datatypeid = datatypeid, limit = limit, offset = offset)
        temp = json.loads((requests.request("GET", url, headers=headers)).text)
        tempResults = pd.DataFrame(temp["results"])
        dataFrame = dataFrame.append(tempResults)
    return dataFrame

# fetch station metadata
def fetch_station_meta_info():
    datasetid = 'GHCND'
    locationid= 'FIPS:24' # maryland
    limit = 1000
    baseUrl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?datasetid={datasetid}&locationid={locationid}&limit={limit}"

    headers = {
            'token': 'YmWIsqbWVOByimkultmIWeLGAztzSjCa'
        }

    url = baseUrl.format(datasetid=datasetid, locationid = locationid, limit = limit)
    response = requests.request("GET", url, headers=headers)

    results = json.loads(response.text)

    totalCount = results["metadata"]["resultset"]["count"]
    print('**** number of stations *** :', totalCount)
    dataFrame = pd.DataFrame(results["results"])
    return dataFrame

# function for sending data to Kinesis at the absolute maximum throughput
def send_kinesis(kinesis_client, kinesis_stream_name, kinesis_shard_count, data):


    kinesisRecords = [] # empty list to store data

    (rows, columns) = data.shape # get rows and columns off provided data

    currentBytes = 0 # counter for bytes

    rowCount = 0 # as we start with the first

    totalRowCount = rows # using our rows variable we got earlier

    sendKinesis = False # flag to update when it's time to send data
    
    shardCount = 1 # shard counter

    # loop over each of the data rows received 
    for _, row in data.iterrows(): 

        values = '|'.join(str(value) for value in row) # join the values together by a '|'

        encodedValues = bytes(values, 'utf-8') # encode the string to bytes

        # create a dict object of the row
        kinesisRecord = {
            "Data": encodedValues, # data byte-encoded
            "PartitionKey": str('aa-bb') # some key used to tell Kinesis which shard to use
        }


        kinesisRecords.append(kinesisRecord) # add the object to the list
        stringBytes = len(values.encode('utf-8')) # get the number of bytes from the string
        currentBytes = currentBytes + stringBytes # keep a running total

        # check conditional whether ready to send
        if len(kinesisRecords) == 500: # if we have 500 records packed up, then proceed
            sendKinesis = True # set the flag

        if currentBytes > 50000: # if the byte size is over 50000, proceed
            sendKinesis = True # set the flag

        if rowCount == totalRowCount - 1: # if we've reached the last record in the results
            sendKinesis = True # set the flag

        # if the flag is set
        if sendKinesis == True:
            
            # put the records to kinesis
            response = kinesis_client.put_records(
                Records=kinesisRecords,
                # Data= encodedValues, # data byte-encoded
                # PartitionKey=str(shardCount), # some key used to tell Kinesis which shard to use

                StreamName = kinesis_stream_name
            )
            
            # resetting values ready for next loop
            kinesisRecords = [] # empty array
            sendKinesis = False # reset flag
            currentBytes = 0 # reset bytecount
            
            # increment shard count after each put
            shardCount = shardCount + 1
        
            # if it's hit the max, reset
            if shardCount > kinesis_shard_count:
                shardCount = 1
            
        # regardless, make sure to incrememnt the counter for rows.
        rowCount = rowCount + 1
        
    
    # log out how many records were pushed
    print('Total Records sent to Kinesis: {0}'.format(totalRowCount))

# main function
def main():
    
    # start timer
    start = time. time()
    
    # create a client with kinesis
    # kinesis-labs-project
    kinesis = create_client('kinesis','us-east-1')

    # fetch stattion meta info for FIPS:27 (Maryland)
    stations_info = fetch_station_meta_info()

    # fetch weather data
    dataFrame = fetch_weather_data()
    print('**** total number of records to be processed',len(dataFrame.values.tolist()))

    # transform data
    data = transform_data(dataFrame, stations_info)

    # send it to kinesis data stream
    stream_name = "kinesis-labs-project-3"
    stream_shard_count = 1
    
    send_kinesis(kinesis, stream_name, stream_shard_count, data) # send it!
    
    # end timer
    end = time. time()
    
    # log time
    print("Runtime: " + str(end - start))
    
if __name__ == "__main__":
    
    # run main
    main()
