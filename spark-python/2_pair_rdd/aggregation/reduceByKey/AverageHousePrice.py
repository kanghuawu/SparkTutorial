import findspark
from pyspark import SparkContext, SparkConf

findspark.init(python_path='/Users/khwu/.virtualenvs/spark/bin/python3')

if __name__ == '__main__':
    '''
    Create a Spark program to read the house data from in/RealEstate.csv,
    output the average price for houses with different number of bedrooms.

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

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

    3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.

    '''
    conf = SparkConf().setAppName('real_estate').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    res = sc.textFile('../../../in/RealEstate.csv') \
        .filter(lambda line: not line.startswith('MLS,Location')) \
        .map(lambda line: (int(line.split(',')[3]), (float(line.split(',')[2]), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .collectAsMap()

    for k in sorted(res.keys()):
        print('({}, {:.2f})'.format(k, res[k][0] / res[k][1]))