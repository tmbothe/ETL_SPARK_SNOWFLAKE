# Project: Data Pipeline: Extract raw data from csv to Snowflake using pyspark on databricks
## Project Description

A company wants a proof of value (POV) project using Snowflake and Databricks. This project constist of loading data from a shared location to snowflake using pyspark on databricks. Three files are provided : airports, airlines, and flights.
Finally, create views or run reports to gather insights based on the datasets. It is recommended to materialize the reports into a table within Snowflake. Any ETL needs to happen within Spark using the Snowflake connector.

The goal of the ETL pipeline is to load data that will help deriving insights such as:
- Total number of flights by airline and airport on a monthly basis
- On time percentage of each airline for the year 2015
- Airlines with the largest number of delays
- Cancellation reasons by airport
- Delay reasons by airport
- Airline with the most unique routes

## Data description
There are three sources of dataset, the **airlines** and the **airports** , and **Flights**.  These datasets are currently stored in a shared location. These files will be read from pySpark pn databricks, trensform load into snowFlake.

### The airlines dataset
This is a csv file loaded with airlines information. 
**columns:**
  - IATA_CODE: string (nullable = true)
  - AIRLINE: string (nullable = true)

### Airport dataset
This is a csv file loaded with airports information.
**columns:**
 - IATA_CODE 
 - AIRPORT 
 - CITY 
 - STATE 
 - COUNTRY 
 - LATITUDE 
 - LONGITUDE

### Flights dataset
This is a csv file loaded with airports information.
**columns:**
  - YEAR: integer (nullable = true)
  - MONTH: integer (nullable = true)
  - DAY: integer (nullable = true)
  - DAY_OF_WEEK: integer (nullable = true)
  - AIRLINE: string (nullable = true)
  - FLIGHT_NUMBER: integer (nullable = true)
  - TAIL_NUMBER: string (nullable = true)
  - ORIGIN_AIRPORT: string (nullable = true)
  - DESTINATION_AIRPORT: string (nullable = true)
  - SCHEDULED_DEPARTURE: integer (nullable = true)
  - DEPARTURE_TIME: integer (nullable = true)
  - DEPARTURE_DELAY: integer (nullable = true)
  - TAXI_OUT: integer (nullable = true)
  - WHEELS_OFF: integer (nullable = true)
  - SCHEDULED_TIME: integer (nullable = true)
  - ELAPSED_TIME: integer (nullable = true)
  - AIR_TIME: integer (nullable = true)
  - DISTANCE: integer (nullable = true)
  - WHEELS_ON: integer (nullable = true)
  - TAXI_IN: integer (nullable = true)
  - SCHEDULED_ARRIVAL: integer (nullable = true)
  - ARRIVAL_TIME: integer (nullable = true)
  - ARRIVAL_DELAY: integer (nullable = true)
  - DIVERTED: integer (nullable = true)
  - CANCELLED: integer (nullable = true)
  - CANCELLATION_REASON: string (nullable = true)
  - AIR_SYSTEM_DELAY: integer (nullable = true)
  - SECURITY_DELAY: integer (nullable = true)
  - AIRLINE_DELAY: integer (nullable = true)
  - LATE_AIRCRAFT_DELAY: integer (nullable = true)
  - WEATHER_DELAY: integer (nullable = true) 

## Choice of Data Model

For this project, airports and airlines are dimension tables and Flights a fact table that explains how the business is run on a daily basis. We will augment the schema with a Calendar dimension to be able to add column sucb as month name and day name thant can look better on reports.

 ### Star Schema Design       
 ![image](https://raw.githubusercontent.com/tmbothe/ETL_SPARK_SNOWFLAKE/main/images/datamodel.PNG)
 
 ## Project Structure
 
 ```
 
   ETL_SPARK_SNOWFLAKE
    |
    |   data
    |      | airports.csv
    |      | airlines.csv
    |      | Flights.csv
    |   images
    |      | datamodel.PNG
    |      | report1.PNG
    |      | report2.PNG      
    |      | datacheck.png
    |      | 
    |  src  
    |      | Analytics_queries.py
    |      | CREATE_TABLE.SQL
    |      | airlines_reports.ipynb
    |      | etl.ipynb
    |      | etl.py  
    |  README.md
 ``` 

## Installation 

- Create an account in Databrick community edition [Databricks](https://community.cloud.databricks.com)
- Create free edition snowflake account [snowflake](https://signup.snowflake.com)
- Clone the current repository 
- Load tables into databricks
- Build snowflake option in databricks to be able to connect to snowflake from databricks.
- Log in to snowflake, create a database and run the `create_table.sql` script to create table in snowflake.
- Upload the jupyter open into databricks (`etl.ipynb`) or open a new jupyter notebook to follow instructions.

This process with load data into snowflake
 
 **Result:**
The table below is the row count check when the data load is done.
  ![image](https://raw.githubusercontent.com/tmbothe/ETL_SPARK_SNOWFLAKE/main/images/datacheck.PNG)


**Example of reports**

Below is an example of report; it is the **distribution of on time airline in 2015**
![image](https://raw.githubusercontent.com/tmbothe/ETL_SPARK_SNOWFLAKE/main/images/report1.PNG)
 



