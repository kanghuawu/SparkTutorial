import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')


def split_comma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return (splits[2], splits[3])

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text;
    generate a pair RDD with airport name being the key and country name being the value.
    Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located,
    IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    ("Kamloops", "Canada")
    ("Wewak Intl", "Papua New Guinea")
    ...

    '''

    conf = SparkConf().setAppName('airports_not_in_usa_pair_rdd').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sc.textFile('../../in/airports.text') \
        .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] != '\"United States\"') \
        .map(lambda line: split_comma(line)) \
        .saveAsTextFile('../../out/airports_not_in_usa_pair_rdd.text')
