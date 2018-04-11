import findspark
from pyspark import SparkContext, SparkConf
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')


def split_comma(line: str) -> str:
    splits = Utils.COMMA_DELIMITER.split(line)
    return '{}, {}'.format(splits[1], splits[2])

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''

    conf = SparkConf().setAppName('airports_in_usa').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    sc.textFile('../../in/airports.text') \
        .filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] == '"United States"') \
        .map(lambda line: split_comma(line)) \
        .saveAsTextFile('../../out/airports_in_usa.text')