import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')


def split_comma(line: str) -> str:
    splits = Utils.COMMA_DELIMITER.split(line)
    return '{}, {}'.format(splits[1], splits[6])

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    ...
    '''

    conf = SparkConf().setAppName('airport_by_latitude').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sc.textFile('../../in/airports.text') \
        .filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40) \
        .map(lambda line: split_comma(line)) \
        .saveAsTextFile('../../out/airports_by_latitude.text')