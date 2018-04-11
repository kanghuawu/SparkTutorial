import findspark
findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')
from pyspark.sql import SparkSession
from common.Utils import Utils

def get_line(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return [splits[2], splits[6], splits[9], splits[14]]

if __name__ == '__main__':
    session = SparkSession.builder.appName('rdd_dataframe_conversion').master('local[*]').getOrCreate()

    sc = session.sparkContext
    sc.setLogLevel('ERROR')

    HEADER = ',collector,country'
    re = sc.textFile('../in/2016-stack-overflow-survey-responses.csv')
    col_names = re.filter(lambda line: line.startswith(HEADER)) \
        .map(get_line)\
        .collect()
    data = re.filter(lambda line: not line.startswith(HEADER)) \
        .map(get_line)\
        .map(lambda row: [(float(v) if (i == 1 or i == 3) and v else v) for i, v in enumerate(row)]) \
        .toDF(col_names[0])

    print('=== Print out schema ===')
    data.printSchema()

    print('=== Print 20 records of responses table ===')
    data.show()

    print('=== Revert to RDD ===')
    for res_rdd in data.rdd.take(10):
        print(res_rdd)