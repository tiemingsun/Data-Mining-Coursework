from pyspark import SparkContext, SparkConf
import json
import sys
from operator import add


def my_hash(x):
    return hash(x[:1])


if __name__ == "__main__":
    # system arguments inputs
    review_json_path = sys.argv[1]
    output_file_path = sys.argv[2]
    partition_type = sys.argv[3]
    n_partitions = int(sys.argv[4])
    n = int(sys.argv[5])

    # spark-submit task3.py ///Users/tieming/inf553/hw1-pyspark/review.json ///Users/tieming/inf553/hw1-pyspark/output_3.json customized 27 40
    # review_json_path = "///Users/tieming/inf553/hw1-pyspark/review.json"
    # output_file_path = "///Users/tieming/inf553/hw1-pyspark/output_3.json"
    # n = 40
    # n_partitions = 27
    # partition_type = "customized"

    sc = SparkContext(master="local", appName="First_app")

    input_review = sc.textFile(review_json_path)
    input_review = input_review.map(lambda lines: json.loads(lines))

    business_id = input_review.map(lambda line: (line['business_id'], 1) )

    if partition_type == "customized":
        business_id = business_id.partitionBy(n_partitions, partitionFunc=my_hash)
        partition_list = business_id.glom().map(len).collect()
        business_larger_than_n = business_id.reduceByKey(add).filter(lambda x: x[1] > n).collect()

    else:
        n_partitions = business_id.getNumPartitions()
        partition_list = business_id.glom().map(len).collect()
        business_larger_than_n = business_id.reduceByKey(add).filter(lambda x: x[1] > n).collect()

    result_dict = {}
    result_dict["n_partitions"] = n_partitions
    result_dict["n_items"] = partition_list
    result_dict["result"] = business_larger_than_n

    with open(output_file_path, 'w+') as output_file:
        json.dump(result_dict, output_file)
    output_file.close()
