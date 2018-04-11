import findspark
from pyspark import SparkContext, SparkConf
import csv
from common.Utils import Utils

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

def load_post_code():
    with open('../../in/uk-postcode.csv') as f:
        reader = csv.reader(f)
        return {row[0]: row[7] for row in reader if not row[0].startswith('Postcode')}


def get_post_prefix(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    post_code = splits[4]
    return post_code.split(' ')[0]


if __name__ == '__main__':
    conf = SparkConf().setAppName('ukpostcode').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    post_code = sc.broadcast(load_post_code())

    HEADER = 'Timestamp,Collected by,Name of makerspace'
    res = sc.textFile('../../in/uk-makerspaces-identifiable-data.csv') \
        .filter(lambda line: not line.startswith(HEADER) and Utils.COMMA_DELIMITER.split(line)[4] != '') \
        .map(lambda line: post_code.value[get_post_prefix(line)] \
            if get_post_prefix(line) in post_code.value else 'Unknown') \
        .countByValue()

    for k, v in res.items():
        print('{} : {}'.format(k, v))