from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DateType

from datetime import datetime, timedelta
from label_mappings import travel, visa, country, port



travel_udf = udf(lambda x: travel[x], StringType())


visa_udf = udf(lambda x: visa[x], StringType())


country_udf = udf(lambda x: country[x], StringType())


port_udf = udf(lambda x: port[x], StringType())



def convert_sasdate(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


udf_sasdate = udf(lambda x: convert_sasdate(x), DateType())


def convert_stringdate(x):
    try:
        parsed_str = datetime.strptime(x, '%m%d%Y')
        return parsed_str.date()
    except:
        return None


udf_strdate = udf(lambda x: convert_stringdate(x), DateType())
