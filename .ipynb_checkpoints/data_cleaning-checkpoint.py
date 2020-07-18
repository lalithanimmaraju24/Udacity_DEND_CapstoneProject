from pyspark.sql.functions import *

from label_mappings import port, states
from user_defined_function import travel_udf, visa_udf, country_udf,\
    port_udf, udf_sasdate, udf_strdate


def state_data(spark, input_data, output_data):
    cities_path = input_data + 'us-cities-demographics.csv'
    cities = spark.read.format("csv").option("header", "true")\
        .option("delimiter", ";").load(cities_path)

    cities = cities\
        .withColumn('Median Age', col('Median Age').cast('double'))\
        .withColumn('Average Household Size', col('Average Household Size').cast('double'))\
        .withColumn('Male Population', col('Male Population').cast('long'))\
        .withColumn('Female Population', col('Female Population').cast('long'))\
        .withColumn('Total Population', col('Total Population').cast('long'))\
        .withColumn('Number of Veterans', col('Number of Veterans').cast('long'))\
        .withColumn('Foreign-born', col('Foreign-born').cast('long'))\
        .withColumn('Count', col('Count').cast('long'))\
        .orderBy('State', ascending=True)

    cities = cities.groupBy(
        'City', 'State', 'State Code', 'Median Age',
        'Average Household Size', 'Male Population',
        'Female Population', 'Total Population',
        'Number of Veterans', 'Foreign-born').pivot('Race').sum('Count')

   
    cities = cities.drop('City')
    states = cities.groupBy('State', 'State Code').agg({
        'Median Age': 'mean',
        'Average Household Size': 'mean',
        'Male Population': 'sum',
        'Female Population': 'sum',
        'Total Population': 'sum',
        'Number of Veterans': 'sum',
        'Foreign-born': 'sum',
        'American Indian and Alaska Native': 'sum',
        'Asian': 'sum',
        'Black or African-American': 'sum',
        'Hispanic or Latino': 'sum',
        'White': 'sum'}).orderBy('state')

    
    states = states.select(
        col('State Code').alias('state_code'),
        col('State').alias('state'),
        col('avg(Median Age)').alias('median_age'),
        col('avg(Average Household Size)').alias('avg_household_size'),
        col('sum(Total Population)').alias('total_population'),
        col('sum(Female Population)').alias('female_population'),
        col('sum(Male Population)').alias('male_population'),
        col('sum(Number of Veterans)').alias('veterans'),
        col('sum(Foreign-born)').alias('foreign_born'),
        col('sum(American Indian and Alaska Native)').alias('native_american'),
        col('sum(Asian)').alias('asian'),
        col('sum(Black or African-American)').alias('black'),
        col('sum(Hispanic or Latino)').alias('hispanic_latino'),
        col('sum(White)').alias('white'))

    
    states = states\
        .withColumn('male_population_pct', states['male_population'] / states['total_population'] * 100)\
        .withColumn('female_population_pct', states['female_population'] / states['total_population'] * 100)\
        .withColumn('veterans_pct', states['veterans'] / states['total_population'] * 100)\
        .withColumn('foreign_born_pct', states['foreign_born'] / states['total_population'] * 100)\
        .withColumn('black_pct', states['black'] / states['total_population'] * 100)\
        .withColumn('white_pct', states['white'] / states['total_population'] * 100)\
        .withColumn('hispanic_latino_pct', states['hispanic_latino'] / states['total_population'] * 100)\
        .withColumn('asian_pct', states['asian'] / states['total_population'] * 100)\
        .withColumn('native_american_pct', states['native_american'] / states['total_population'] * 100)

    # Drop not needed column
    columns_to_drop = [
        'total_population', 'female_population', 'male_population', 'veterans',
        'foreign_born', 'native_american', 'asian', 'black', 'hispanic_latino',
        'white']
    states = states.drop(*columns_to_drop)

    # Round numbers to 2 decimal places
    states = states\
        .withColumn('median_age', round(states['median_age'], 2))\
        .withColumn('avg_household_size', round(states['avg_household_size'], 2))\
        .withColumn('male_population_pct', round(states['male_population_pct'], 2))\
        .withColumn('female_population_pct', round(states['female_population_pct'], 2))\
        .withColumn('veterans_pct', round(states['veterans_pct'], 2))\
        .withColumn('foreign_born_pct', round(states['foreign_born_pct'], 2))\
        .withColumn('black_pct', round(states['black_pct'], 2))\
        .withColumn('white_pct', round(states['white_pct'], 2))\
        .withColumn('hispanic_latino_pct', round(states['hispanic_latino_pct'], 2))\
        .withColumn('asian_pct', round(states['asian_pct'], 2))\
        .withColumn('native_american_pct', round(states['native_american_pct'], 2))

    # write states dimenstion table to parquet files
    print('Started processing "states dimension table" parquet files...')
    states.write.parquet(output_data + 'states.parquet', mode='overwrite')
    print('Processed all "states dimension table" parquet files.')

    return states


def airport_data(spark, input_data, output_data):
    # Load data
    airport_path = input_data + 'airport-codes_csv.csv'
    airports = spark.read.format("csv").option("header", "true").load(airport_path)

    # Filter for only US airports and create state_code column from iso-region.
    airports = airports.filter(airports["iso_country"] == 'US')\
        .withColumn("state_code", substring(airports['iso_region'], 4, 2))

    # Drop not needed columns
    columns_to_drop = [
        'elevation_ft', 'continent', 'iso_country', 'iso_region', 'gps_code',
        'local_code']
    airports = airports.drop(*columns_to_drop)

    # Change order of columns and drop duplicates
    airports = airports.select(
        'ident', 'iata_code', 'type', 'name', 'state_code', 'municipality',
        'coordinates').dropDuplicates()

    # write airports dimenstion table to parquet files
    print('Started processing "airports dimension table" parquet files...')
    airports.write.parquet(output_data + 'airports.parquet', mode='overwrite')
    print('Processed all "airports dimension table" parquet files.')

    return airports


def immigration_data(spark, input_data, output_data):
    # Load data
    immigration_path = input_data + 'sas_data'
    immigration = spark.read.parquet(immigration_path)

    # Drop not needed columns
    columns_to_drop = [
        'i94cit', 'count', 'dtadfile', 'visapost', 'entdepa', 'entdepd',
        'entdepu', 'matflag', 'insnum', 'airline', 'admnum', 'fltno']
    immigration = immigration.drop(*columns_to_drop)

    # Filter for records without empty state (i94res) fields
    # Filter for records without empty country of origin (i94addr) fields
    # Choose records only with port and state fields matching list of existing labels
    immigration = immigration\
        .filter(immigration.i94addr.isNotNull())\
        .filter(immigration.i94res.isNotNull())\
        .filter(col("i94port").isin(list(port.keys())))\
        .filter(col("i94addr").isin(list(states.keys())))

    # Convert column codes to their full names as per UDFs in label_mappings.py file
    immigration = immigration\
        .withColumn('i94res', country_udf(immigration['i94res']))\
        .withColumn('port_name', port_udf(immigration['i94port']))\
        .withColumn('i94mode', travel_udf(immigration['i94mode']))\
        .withColumn('i94visa', visa_udf(immigration['i94visa']))

    # Assign correct data types
    immigration = immigration\
        .withColumn('cicid', col('cicid').cast('long'))\
        .withColumn('i94yr', col('i94yr').cast('integer'))\
        .withColumn('i94mon', col('i94mon').cast('integer'))\
        .withColumn('i94bir', col('i94bir').cast('integer'))\
        .withColumn('biryear', col('biryear').cast('integer'))\
        .withColumn('arrdate', udf_sasdate(immigration['arrdate']))\
        .withColumn('depdate', udf_sasdate(immigration['depdate']))\
        .withColumn('dtaddto', udf_strdate(immigration['dtaddto']))

    # Rename and set the order of columns
    immigration = immigration.select(
        'cicid',
        col('i94yr').alias('year'),
        col('i94mon').alias('month'),
        col('i94addr').alias('state_code'),
        col('i94port').alias('port_entry'),
        'port_name',
        col('i94mode').alias('mode_travel'),
        col('i94res').alias('country_origin'),
        col('i94bir').alias('age'),
        col('biryear').alias('birth_year'),
        'gender',
        col('occup').alias('occupation'),
        col('i94visa').alias('visa_code'),
        col('visatype').alias('visa_type'),
        col('arrdate').alias('arrival_date'),
        col('dtaddto').alias('allowed_stay_date'),
        col('depdate').alias('departure_date'))

    # write fact table to parquet files
    print('Started processing "immigration dimension table" parquet files...')
    immigration.write.parquet(
        output_data + 'immigration.parquet',
        mode='overwrite', partitionBy=('year', 'month'))
    print('Processed all "immigration dimension table" parquet files.')

    return immigration
