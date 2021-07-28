CREATE TABLE AIRPORT(
IATA_CODE string,
 AIRPORT string,
 CITY string,
 STATE string,
 COUNTRY string,
 LATITUDE double,
 LONGITUDE double
 );
 
 CREATE TABLE AIRLINES (
   IATA_CODE string,
   AIRLINE string 
 );
 
 CREATE FLIGHTS(
 YEAR integer ,
 MONTH integer ,
 DAY integer ,
 DAY_OF_WEEK integer ,
 AIRLINE string ,
 FLIGHT_NUMBER integer ,
 TAIL_NUMBER string ,
 ORIGIN_AIRPORT string ,
 DESTINATION_AIRPORT string ,
 SCHEDULED_DEPARTURE integer ,
 DEPARTURE_TIME integer ,
 DEPARTURE_DELAY integer ,
 TAXI_OUT integer ,
 WHEELS_OFF integer ,
 SCHEDULED_TIME integer ,
 ELAPSED_TIME integer ,
 AIR_TIME integer ,
 DISTANCE integer ,
 WHEELS_ON integer ,
 TAXI_IN integer ,
 SCHEDULED_ARRIVAL integer ,
 ARRIVAL_TIME integer ,
 ARRIVAL_DELAY integer ,
 DIVERTED integer ,
 CANCELLED integer ,
 CANCELLATION_REASON string ,
 AIR_SYSTEM_DELAY integer ,
 SECURITY_DELAY integer ,
 AIRLINE_DELAY integer ,
 LATE_AIRCRAFT_DELAY integer ,
 WEATHER_DELAY integer 
 )
 CLUSTER BY (YEAR,MONTH,DAY)
 ;
 
 CREATE OR REPLACE TABLE Calendar(
  YEAR integer,
  MONTH,
  DAY,
  DAY_OF_WEEK,
  MONTH_NAME,
  DAY_NAME
  );