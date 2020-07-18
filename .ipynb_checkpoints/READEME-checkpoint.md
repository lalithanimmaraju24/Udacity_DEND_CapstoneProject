## Udacity Data Engineering Capstone Project

## The scope of the project

The main aim of this project is to use PySpark to combine data from the following sources to create a star schema that can be used to analyze migration to or from the US and its impact on the U.S. states demographics.

U.S. city demographic and airport data is stored in CSV files, whereas I94 immigration data is distributed among many snappy parquet files.
    
Spark was chosen for this project due to few reasons - 
* efficiency and speed of processing large amounts of data/files as it allows for parallelization of computing. 
* It also scales easily with new additional nodes which gives flexibility in case an input data grows significantly. 
* Lastly, PySpark is able to read many different data formats which are input data in this project. It also allows for an easy data manipulation and usage of standard SQL queries like joins which were used to create new tables
    
    
The goal of this project is to design an ETL pipeline that extracts information from these files, transforms it to fit the predefined data schema and loads the tables into either local folder or to an Amazon S3 bucket for a downstream analytics team to consume.



## Organization of the code and its execution
Just Run `etl.py` to execute the ETL pipeline as well as QC checks.

- `dl.cfg` you can also config file that contains AWS credentials.
- etl.py` main ETL script that creates Spark session, executes data_cleaning.py and subsequently creates fact table. The output of fact table is also written into "tables" folder as a set of parquet files. Lastly, it executes 2 quality checks.
- `user_defined_function.py` contains set of user defined functions (UDFs) which are imported and executed in data_cleaning.py.
- `label_mappings.py` contains set of dictionaries described in the "Label mapppings" section. They are imported in user_defined_functions.py and 2 of them also in data_cleaning.py.
- `data_cleaning.py` executes pyspark script which extracts data from files in the "data" folder, transforms that data into dimensional tables and writes these tables into "tables" folder (or S3 bucket) as a set of parquet files. It is imported into etl.py
   

## Overview of data

### Raw Data

Stored in `data\` folder:

`I94 Immigration data` which comes from [the US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html). There are 14 snappy parquet files with a total of 3,096,313 records and 29 columns on U.S. immigration statistics. 

`U.S. City Demographic Data` which comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). It is a CSV file with 2891 records and 12 columns describing U.S. city demographic characteristics. 

`Airport Code Table` which comes from [datahub.io](https://datahub.io/core/airport-codes#data). It is a CSV file with 55,075 records and 12 columns describing basic characteristics of worldwide airports.

### Label mappings

`travel` dictionary of U.S. I94 immigration modes of travel
`states` dictionary of state names
`country` dictionary of U.S. I94 immigration country labels
`port` dictionary of U.S. port of entry labels
`visa` dictionary of U.S. visa types

## Schema design
Star schema with 3 dimensional tables and 1 fact table. The design of dimension and fact tables is optimized for analytical queries aimed at analyzing flow of migration to/from the United States and its impact on the state demographics.


### Fact Table
**`states_immigration` has data associated with main demographic measures of states and immigrations statistics as well as number of international ports of entry. It is partitioned by year and month.**
Consists of columns: 

|columns| data type| description| compulsory ? | 
|---|---|---|---|
| year |*integer* | year | compulsory | 
| month |*integer*   | month  | compulsoory |
| state_code |*string*  | state code   | compulsory  |
| state  | *string*  | state name  | compulsory  |
| int_airport_state_count |*long*   | number of international airports in a state  |   |
| country_origin  |*string*   | country of origin of a visitor/immigrant  | compulsory  |
| foreign_born_pct  | *double*  | percent of foreign born population in a state   |   |
| black_pct  |*double*   | percent of black population in a state  |   |
| white_pct | *double*  | percent of white population in a state  |   |
| hispanic_latino_pct |*double*   | percent of hispanic/latino population in a state  |   |
| asian_pct  |*double*   | percent of asian population in a state  |   |
| native_american_pct | *double*  | percent of native american population in a state   |   |


### Dimension Tables
**`states` has demographic data aggregated on a level of US states. Consists of columns:**

|columns| data type| description| compulsory ? | 
|---|---|---|---|
| state_code |*string* |state code | compulsory | 
| state |*string*   | state name  | compulsory |
| median_age |*double*   |median age in a state   |   |
| avg_household_size  | *double*  |average household size in a state  |   |
| male_population_pct |*double*   | percent of male population in a state  |   |
| female_population_pct  |*double*   | percent of female population in a state  |   |
| veterans_pct  | *double*  |percent of veterans population in a state   |  |
| foreign_born_pct  | *double*  | percent of foreign born population in a state   |   |
| black_pct  |*double*   | percent of black population in a state  |   |
| white_pct | *double*  | percent of white population in a state  |   |
| hispanic_latino_pct |*double*   | percent of hispanic/latino population in a state  |   |
| asian_pct  |*double*   | percent of asian population in a state  |   |
| native_american_pct | *double*  | percent of native american population in a state   |   |


**`airports` has data on airport characteristics. Consists of columns:**

|columns| data type| description| compulsory? | 
|---|---|---|---|
| ident |*string* | idenfitication code of an airport | compulsory | 
| iata_code |*string*  | IATA location identifier | |
| type |*string*  | airport type |   |
| name | *string*  |airport name|   |
| state_code| *string*   | state code  | compulsory   |
| municipality  | *string*   | municipality name  |   |
| coordinates   | *string*  | airport coordinates |  |


**`immigration` has data on U.S. immigration statistics. It is partitioned by year and month. Consists of columns:**

|columns| data type| description| compulsory ? | 
|---|---|---|---|
| cicid |*long* | immigration record indentifier | compulsory | 
| year |*integer* | year of immigration record| compulsory | 
| month |*integer*   | month of immigration record | compulsory |
| state_code |*string*  | state code   | compulsory  |
| port_of_entry  | *string*  | code of a port of entry to the U.S.  |  |
| port_name |*string*  | name of a port of entry to the U.S.   |   |
| mode_travel  |*string*   | mode of travel of a visitor/immigrant  |  |
| country_origin  | *string*  | country of origin of a visitor/immigrant   | compulsory |
| age  |*integer*   | age of a visitor/immigrant   |   |
| birth_year | *integer*  | birth year of a visitor/immigrant   |   |
| gender |*string*   | gender of a visitor/immigrant   |   |
| occupation  |*string*  | potential occupation of an immigrant in the U.S. |   |
| visa_code | *string*  |visa code  |   |
| visa_type | *string*  | visa type  |   |
| arrival_date| *date*  | arrival date of a visitor/immigrant   |   |
| allowed_stay_date | *date*  | date to which a visitor/immigrant is allowed to stay in the U.S.|   |
| departure_date  | *date*  | departure date of a visitor/immigrant   |   |


## Future enhancements and data scaling

### What if the pipelines were run on a daily basis by 7am. 
Our ETL process could be incorporated into Apache Airflow DAG and scheduled to automatically execute at 7am  If the data would need to be updated or refreshed every 24hrs, .

### What if the database needed to be accessed by 100+ people.
In this case we could utilize Amazon S3(optional of this project). The output of our ETL process parquet files with both fact and dimension tables could be sent to properly set-up and scaled S3 bucket that may be easily accessed by a big number of people (100 or more).

### What if the data was increased by 100x. 
This require much bigger computational power - we could move our Spark instance from being executed locally to the cloud computing platform, e.g. Amazon EMR and increase the number of clusters/nodes that would be able process such amount of data in parallel.  










