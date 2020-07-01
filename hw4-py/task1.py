from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf,SQLContext
import os
import csv
import time
import sys

def readfile(DATA_FILE):
    with open(DATA_FILE, 'r') as file:
        csv_reader = csv.reader(file, delimiter=',')
        out_list = []
        ishead = 1
        for row in csv_reader:
            if ishead:
                ishead = 0
                continue
            else:
                out_list.append(row)
    return out_list    


def find_relation(src, dst, id_dict, THRESHOLD):
    set1, set2 = id_dict[src], id_dict[dst]
    len_intersect = len(set1.intersection(set2))
    if len_intersect >= THRESHOLD:
        return "follow"
    else:
        return "not follow"


def convert_to_file(OUTPUT_PATH, res_list):
    with open(OUTPUT_PATH, 'w') as fp:
        for single_list in res_list:
            fp.write(single_list)
            fp.write("\n")
        fp.close()
    return


start_time = time.time()

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  pyspark-shell")

# DATA_FILE = "///Users/tieming/inf553/hw4-py/data/ub_sample_data.csv"
# OUTPUT_PATH = "///Users/tieming/inf553/hw4-py/task1.txt"
# THRESHOLD = 7
THRESHOLD = int(sys.argv[1])
DATA_FILE = sys.argv[2]
OUTPUT_PATH = sys.argv[3]

conf = SparkConf().setMaster("local")\
        .setAppName("task1")\
        .set("spark.executor.memory", "4g")\
        .set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark_sql = SQLContext(sc)


data_list = readfile(DATA_FILE)

raw_data_rdd = sc.textFile(DATA_FILE).map(lambda x: x.split(',')).filter(lambda x: x[0] != 'user_id')


raw_data_rdd = sc.parallelize(data_list)\
                .groupByKey()\
                .mapValues(lambda x: set(x))

id_dict = raw_data_rdd.collectAsMap()

user_rdd = raw_data_rdd.map(lambda line: line[0]).distinct().coalesce(2)

combined_rdd = user_rdd.cartesian(user_rdd)\
                        .filter(lambda line: line[0] < line[1])

graph_rdd = combined_rdd.map(lambda line: (line[0], line[1], find_relation(line[0], line[1], id_dict, THRESHOLD)) )\
            .filter(lambda line: line[2] == "follow")


user_distinct = graph_rdd.map(lambda line: (line[0], line[1]))\
            .flatMap(lambda x: x)\
            .distinct()\
            .map(lambda x: [x])

graph_with_reverse = graph_rdd.map(lambda line: ([line[0], line[1]], [line[1], line[0]]) )\
            .flatMap(lambda x: x)


vertex_df = spark_sql.createDataFrame(user_distinct, ["id"]).coalesce(6)
edge_df = spark_sql.createDataFrame(graph_with_reverse,["src", "dst"]).coalesce(6)

time_here = time.time()
g = GraphFrame(vertex_df, edge_df)

res_df = g.labelPropagation(maxIter=5)
# res_df.show()
print("Duration for graph propagtion:", time.time() - time_here)

res_rdd = res_df.coalesce(1).rdd.map(tuple)

res_list = res_rdd.map(lambda pair: (pair[1], pair[0]))\
        .groupByKey()\
        .map(lambda line: sorted(list(line[1])))\
        .sortBy(lambda x: [len(x), x] )\
        .map(lambda x: ", ".join(x))\
        .collect()

convert_to_file(OUTPUT_PATH, res_list)
print("Duration: ", time.time() - start_time)