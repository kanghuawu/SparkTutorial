import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark import SparkContext, SparkConf


if __name__ == '__main__':

    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''

    conf = SparkConf().setAppName('union_host').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    HEADER = 'host	logname	time	method	url	response	bytes'

    nasa_july = sc.textFile('../../in/nasa_19950701.tsv') \
        .filter(lambda line: line != HEADER) \
        .map(lambda line: line.split('\t')[0])

    nasa_aug = sc.textFile('../../in/nasa_19950801.tsv') \
        .filter(lambda line: line != HEADER) \
        .map(lambda line: line.split('\t')[0])

    nasa_july.union(nasa_aug) \
        .sample(True, 0.1) \
        .saveAsTextFile('../../out/sample_nasa_logs.csv')