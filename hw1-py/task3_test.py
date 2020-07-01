from pyspark import SparkContext, SparkConf
import os
import json
import sys
from operator import add


sc = SparkContext(master="local", appName="First_app")

review_json_path = "///Users/tieming/inf553/hw1-pyspark/review.json"


input_review = sc.textFile(review_json_path)
input_review = input_review.map(lambda lines: json.loads(lines))
business_id = input_review.map(lambda line: (line['business_id'], 1) )

# by default
n = 15
res_num = business_id.reduceByKey(add).filter(lambda x: x[1] > n).count()
print('by default', res_num)


# customerized 
# business_id.partitionBy(27, partitionFunc=lambda x: ord(x[0]))
# n = 15
# res_num = business_id.reduceByKey(add).filter(lambda x: x[1] > n).count()
# print('customized', res_num)