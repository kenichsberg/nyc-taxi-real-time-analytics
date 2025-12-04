# NYC Taxi Real-Time Analytics

This project demonstrates a real-time analytics pipeline for GPS data generated from the NYC taxi dataset, simulating live streaming and enabling an interactive dashboard.<br/>
Since the original NYC taxi dataset does not contain geolocation data, I designed and implemented the simulated GPS data generation and transmission, allowing the rest of the pipeline to operate as if it were processing real GPS data.<br />
<br/>
*Using*: **Apache Spark, Apache Kafka, Apache Pinot, Metabase, Grafana**
 <img width="935" height="915" alt="dashboard" src="https://github.com/user-attachments/assets/fc43b0b4-9b26-40d3-ac8f-050d512d5541" />\
 <img width="935" height="832" alt="fare-analysis" src="https://github.com/user-attachments/assets/1bc91a16-c4db-415e-b672-fad194edd7f8" />



## Architecture
<img width="1001" height="482" alt="NYC Taxi drawio" src="https://github.com/user-attachments/assets/658feeb5-b4fc-40b2-8f6f-f08608e4e0ce" />


## Overview

The system prepares NYC taxi GPS data in advance and ingests, processes, and visualizes it in near real-time. It consists of the following components:

1. **Simulated data generator**
   - Preprocesses [NYC Taxi & Limousine Commission (TLC) data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
     , adding simulated current GPS data calculated by the original pickup/drop-off location information per second or several seconds.
   - The original records contain only the pickup/drop-off location's id, borough, and zone (string).
     However, they also provide a Shapefile([here](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip)) for each LocationID, from which we can calculate geometry.<br/>
     For each original record, the generator picks random geolocation points within the zone geometry for both the pickup/drop-off location points
     and calculates the current location point during the trip as if a virtual taxi goes along the line defined by these 2 points.
   - The generated GPS data is concentrated 100x more than the real taxi dataset, to increase ingestion load. The data is stored in the filesystem in advance.

1. **Data Streaming**
   - A file feeder feeds the generated GPS data files above to **Spark** every second.
   - Spark reads from a file stream and produces events to **Kafka** topics.
  
1. **Data Transformer**
   - A separate Spark instance reads from a Kafka topic, adds borough/zone information from the geometry data to each current location, and writes to another Kafka topic

1. **Datastore**
   - **Apache Pinot** ingests data from the Kafka topics and provides low-latency queries for analytics dashboards.

1. **Analytics**
   - Fully functional dashboard for visualizing analytics by **Metabase** that is automatically updated to the latest state.






   




