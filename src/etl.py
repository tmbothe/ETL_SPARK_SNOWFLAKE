#libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as long
import  pyspark.sql.functions as F
from pyspark.sql import types as t
import os


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_airlines_data(spark,input_data, snowflake_options,snowflake_table):
    """
    This function reads the airlines file and load into a data frame.
    drop the duplicates if exists and load into snowFlake
    """

    airlines_schema = R([
      Fld("IATA_CODE" ,Str()),
      Fld("AIRLINE",Str()),
    ])
  
    airlines = spark.read.csv(input_data,schema=airlines_schema)
    airlines = airlines.drop_duplicates()
  
    #loading to snowflake
    airlines.write.format("snowflake").options(**snowflake_options).option("dbtable", snowflake_table).mode('append').options(header=True).save()


def process_airports_data(spark,input_data, output_data,snowflake_options,snowflake_table):
    """
    This function reads the airports file and load into a data frame.
    drop the duplicates if exists and load into snowFlake
    """
    airports_schema = R([
        Fld("IATA_CODE" ,Str()),
        Fld("AIRPORT",Str()),
        Fld("CITY" ,Str()),
        Fld("STATE",Str()),
        Fld("COUNTRY" ,Str()),
        Fld("LATITUDE",Dbl()),
        Fld("LONGITUDE",Dbl())
    ])
  
    airports = spark.read.csv(input_data,schema=airports_schema)
    airports = airports.drop_duplicates()
  
    #loading to snowflake
    airports.write.format("snowflake").options(**snowflake_options).option("dbtable", snowflake_table).mode('append').options(header=True).save()


def process_flights_data(spark,input_data, snowflake_options,snowflake_table):
  """
  This function reads the flights file and load into a data frame.
  drop the duplicates if exists and load into snowFlake
  """
  flights_schema = R([
    Fld("YEAR" , Int() ),
    Fld("MONTH" , Int() ),
    Fld("DAY" , Int() ),
    Fld("DAY_OF_WEEK" , Int() ),
    Fld("AIRLINE" , Str() ),
    Fld("FLIGHT_NUMBER" , Int() ),
    Fld("TAIL_NUMBER" , Str() ),
    Fld("ORIGIN_AIRPORT" , Str() ),
    Fld("DESTINATION_AIRPORT" , Str() ),
    Fld("SCHEDULED_DEPARTURE" , Int() ),
    Fld("DEPARTURE_TIME" , Int() ),
    Fld("DEPARTURE_DELAY" , Int() ),
    Fld("TAXI_OUT" , Int() ),
    Fld("WHEELS_OFF" , Int() ),
    Fld("SCHEDULED_TIME" , Int() ),
    Fld("ELAPSED_TIME" , Int() ),
    Fld("AIR_TIME" , Int() ),
    Fld("DISTANCE" , Int() ),
    Fld("WHEELS_ON" , Int() ),
    Fld("TAXI_IN" , Int() ),
    Fld("SCHEDULED_ARRIVAL" , Int() ),
    Fld("ARRIVAL_TIME" , Int() ),
    Fld("ARRIVAL_DELAY" , Int() ),
    Fld("DIVERTED" , Int() ),
    Fld("CANCELLED" , Int() ),
    Fld("CANCELLATION_REASON" , Str() ),
    Fld("AIR_SYSTEM_DELAY" , Int() ),
    Fld("SECURITY_DELAY" , Int() ),
    Fld("AIRLINE_DELAY" , Int() ),
    Fld("LATE_AIRCRAFT_DELAY" , Int() ),
    Fld("WEATHER_DELAY" , Int() )
 ])

  
  flights = spark.read.csv(input_data,schema=flights_schema)
  flights = flights.drop_duplicates()
  
  #loading to snowflake
  flights.write.format("snowflake").options(**snowflake_options).option("dbtable", snowflake_table).mode('append').options(header=True).save()


def process_calendar(flights,snowflake_options,snowflake_table):
  """
    This function take as input the fight datarame, select dates columns
    add two columns for month and day of the week
    Then load into snowflake
  """
  
  month_lst = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
  days = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')

  month_name = udf(lambda x: month_lst[int(x%12) - 1], Str())
  day_name = udf(lambda x : days[int(x%7)-1], Str())

  time_table = flights.select("YEAR","MONTH","DAY","DAY_OF_WEEK").drop_duplicates()


  time_table= time_table.withColumn("MONTH_NAME",month_name(col("MONTH")))          
  time_table= time_table.withColumn("DAY_NAME",day_name(col("DAY")))  
  
  #loading to snowflake
  time_table.write.format("snowflake").options(**snowflake_options).option("dbtable", snowflake_table).mode('append').options(header=True).save()


if __name__=='__main__' :

    SNOWFLAKE_OPTIONS = {
        'sfURL': os.environ.get("SNOWFLAKE_URL", "ofa61631.us-east-1.snowflakecomputing.com"),
        'sfAccount': os.environ.get("SNOWFLAKE_ACCOUNT", "ofa61631"),
        'sfUser': os.environ.get("SNOWFLAKE_USER", "TKONCHOU"),
        'sfPassword': os.environ.get("SNOWFLAKE_PASSWORD", "XXXXXX"),
        'sfDatabase': os.environ.get("SNOWFLAKE_DATABASE", "IXXXXX"),
        'sfSchema': os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        'sfWarehouse': os.environ.get("SNOWFLAKE_WAREHOUSE", "XXXXX"),
        'sfRole':  os.environ.get("SNOWFLAKE_ROLE", "accountadmin"),
    }

    spark = create_spark_session()


    process_airlines_data(spark,input_data, snowflake_options,snowflake_table)

    process_airports_data(spark,input_data, snowflake_options,snowflake_table)

    process_flights_data(spark,input_data, snowflake_options,snowflake_table)

    process_calendar(flights,snowflake_options,snowflake_table)








