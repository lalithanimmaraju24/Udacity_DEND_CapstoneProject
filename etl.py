import os
import configparser

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from data_cleaning import state_data, airport_data, immigration_data



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def fact_table(spark, states, airports, immigration, output_data):
    # Create Dimension tables
    airports.createOrReplaceTempView("airports_dim")
    states.createOrReplaceTempView("states_dim")
    immigration.createOrReplaceTempView("immigration_dim")

    states_immigration = spark.sql("""SELECT
        im.year,
        im.month,
        im.state_code,
        st.state,
        COUNT(air.iata_code) AS int_airport_state_count,
        im.country_origin,
        st.foreign_born_pct,
        st.black_pct,
        st.white_pct,
        st.hispanic_latino_pct,
        st.asian_pct,
        st.native_american_pct

        FROM immigration_dim AS im
        JOIN states_dim AS st ON im.state_code = st.state_code
        JOIN airports_dim AS air ON air.state_code = st.state_code

        GROUP BY im.year, im.month, im.state_code, st.state,
        air.iata_code, im.country_origin, st.foreign_born_pct,
        st.black_pct, st.white_pct, st.hispanic_latino_pct,
        st.asian_pct, st.native_american_pct

        ORDER BY im.state_code, im.country_origin
        """)

    # Convert states_immigration fact table to data frame
    states_immigration.toDF(
        'year', 'month', 'state_code', 'state',
        'int_airport_state_count', 'country_origin',
        'foreign_born_pct', 'black_pct', 'white_pct',
        'hispanic_latino_pct', 'asian_pct',
        'native_american_pct')

    # write fact table to parquet files
    print('Started processing "states_immigration fact table" parquet files...')
    states_immigration.write.parquet(
        output_data + 'states_immigration.parquet',
        mode='overwrite', partitionBy=('year', 'month'))
    print('Processed all "states_immigration fact table" parquet files.')

    # QC CHECKS
    print('Running QC checks..')
    # Count the total number rows in states_immigration fact table.
    print('Total number of rows in the fact table:')
    states_immigration.select(
        count('state_code').alias('fact_table_rows_count')).show()

    # Check for null values in main columns of fact table
    print('Verifation of empty values in year, month, ')
    states_immigration.select(
        isnull('year'),
        isnull('month'),
        isnull('country_origin'),
        isnull('state_code')).dropDuplicates().show()


def main():
    spark = create_spark_session()

    input_data = 'data/'
    output_data = 'tables/'
   

    states = state_data(spark, input_data, output_data)
    airports = airport_data(spark, input_data, output_data)
    immigration = immigration_data(spark, input_data, output_data)

    fact_table(spark, states, airports, immigration, output_data)


if __name__ == '__main__':
    main()
