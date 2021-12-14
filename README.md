## data-processing-pipeline
Data Processing Pipeline using AWS Kinesis Data Stream (KDS)

#### producer - python program that can be hosted in EC2 instance.
  - Fetches weather info from data source in JSON format using servier-side-pagination and transforms the data
  - Streams the data as batch of 500 records to Kinesis AWS resource.

#### data source - https://www.programmableweb.com/api/noaa-climate-data-online-rest-api-v2
 - Fectched Weather Data (Preceiptation, Snow, Min & Max Temperatures for Maryland for October 2021.

#### consumer - AWS Lambda
 - Listens to Kinesis Data Stream and processes the data by inserting records into Preceiptation and Temparature dynamo DB tables.


![This is an image](https://github.com/jagadeeshmeesala/data-processing-pipeline/blob/d36fd68bde4d71801f1550232e59507ee81a4a6e/image/Data%20Pipeline_architecture.drawio.png)

