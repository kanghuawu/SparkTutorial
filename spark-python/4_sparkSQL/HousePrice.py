import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark.sql import SparkSession, functions as F


if __name__ == '__main__':
    '''    
    Create a Spark program to read the house data from in/RealEstate.csv,
    group by location, aggregate the average price per SQ Ft and sort by average price per SQ Ft.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

    +----------------+-----------------+
    |        Location| avg(Price SQ Ft)|
    +----------------+-----------------+
    |          Oceano|             95.0|
    |         Bradley|            206.0|
    | San Luis Obispo|            359.0|
    |      Santa Ynez|            491.4|
    |         Cayucos|            887.0|
    |................|.................|
    |................|.................|
    |................|.................|
    '''
    session = SparkSession.builder.appName('house_price').master('local[*]').getOrCreate()
    session.sparkContext.setLogLevel('ERROR')
    data_frame_reader = session.read

    res = data_frame_reader\
        .option('header', 'true')\
        .option('inferSchema', True)\
        .csv('../in/RealEstate.csv')\
        .groupBy('Location')\
        .agg(F.avg('Price SQ Ft'))\
        .orderBy('avg(Price SQ FT)')\
        .show()

    session.stop()

