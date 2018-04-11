import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':

    '''
    Create a Spark program to read the airport data from in/airports.text,
    output the the list of the names of the airports located in each country.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
    "Papua New Guinea",  ["Goroka", "Madang", ...]
    ...

    '''
    conf = SparkConf().setAppName('airport_by_country').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    res = sc.textFile('../../in/airports.text') \
        .map(lambda line: (Utils.COMMA_DELIMITER.split(line)[3], Utils.COMMA_DELIMITER.split(line)[1])) \
        .groupByKey() \
        .mapValues(list) \
        .collectAsMap()

    for k, v in res.items():
        print('{}: {}'.format(k, v))