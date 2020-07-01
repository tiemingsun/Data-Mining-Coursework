from pyspark import SparkContext
import json
import csv

sc = SparkContext(master="local", appName="preprocess")


review_json_path = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/review.json"
business_json_path = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/business.json"
preprocessing_path = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/preprocessing.csv"


input_review = sc.textFile(review_json_path)
input_business = sc.textFile(business_json_path)

input_review = input_review.map(lambda lines: json.loads(lines)).persist()
input_business = input_business.map(lambda lines: json.loads(lines)).persist()

input_business_NV = input_business.map(lambda line: (line['business_id'], line['state']))\
                                  .filter(lambda line: line[1] == "NV")
business_in_state = input_business_NV.map(lambda line: line[0]).collect()
business_in_state = set(business_in_state)

output = input_review.map(lambda line: (line['user_id'], line['business_id']))\
                     .filter(lambda line: line[1] in business_in_state).collect()


with open(preprocessing_path, 'w', newline='') as csvfile:
    yelp_writer = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_NONE)
    yelp_writer.writerow(["user_id", "business_id"])
    for line in output:
        yelp_writer.writerow([line[0], line[1]])
