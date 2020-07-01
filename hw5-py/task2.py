from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
import sys
from datetime import datetime
import json
import random
import csv
from binascii import hexlify
from statistics import mean, median


def hexlify_city(city):
    
    if city:
        city_code = int(hexlify(city.encode('utf8')),16)
    else:
        city_code = int(hexlify("a".encode('utf8')),16)
    
    return city_code


def generate_hash(numOfHash):
    '''
    return a list that contains a number of hash functions
    ''' 
    output_list = []
    for _ in range(numOfHash):
        output_list.append((random.randint(2**3, (2 ** 16) - 1), random.randint(2**3, (2 ** 16) - 1)))
    return output_list


def calc_trailing_zero(city_code,hash_list):
    '''
    :return res_list: a list with size "numOfHash" List(List(hash_index, 2 ^ trailing_zero))
    '''
    res_list = []
    for i in range(len(hash_list)):

        city_hash = str(bin((hash_list[i][0] * city_code + hash_list[i][1]) % (2 ** 8 - 1)))

        zero_num = 0
        for s in reversed(city_hash):
            if s == "0":
                zero_num += 1
            else:
                break
        res_list.append([i, 2 ** zero_num])

    return res_list


def flajolet_martin(rdd, OUTPUT_PATH, numOfHash):
        
    cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    city_list = rdd.collect()
    ground_truth = len(set(city_list))

    hash_list = generate_hash(numOfHash)

    
    max_each_hash = rdd.coalesce(1)\
        .map(lambda city_code: calc_trailing_zero(city_code,hash_list))\
        .flatMap(lambda pair: pair)\
        .groupByKey()\
        .map(lambda line: max(line[1]))\
        .collect()
    # print(max_each_hash)

    # group hash signatures into 6 groups
    group_1 = mean(max_each_hash[:int(numOfHash / 6)])
    group_2 = mean(max_each_hash[int(numOfHash / 6):int(2*numOfHash/6)])
    group_3 = mean(max_each_hash[int(2*numOfHash / 6):int(3*numOfHash/6)])
    group_4 = mean(max_each_hash[int(3*numOfHash / 6):int(4*numOfHash/6)])
    group_5= mean(max_each_hash[int(4*numOfHash / 6):int(5*numOfHash/6)])
    group_6 = mean(max_each_hash[int(5*numOfHash / 6):int(numOfHash)])



    # get median
    estimation = int(median([group_1, group_2, group_3, group_4, group_5, group_6]))
    # print("estimation is {} ;truth is {}".format(estimation, ground_truth))

    csvfile = open(OUTPUT_PATH, 'a')
    csv_writer = csv.writer(csvfile, delimiter=',')
    csv_writer.writerow([cur_time, ground_truth, estimation])
    csvfile.close()
    return


# java -cp ///Users/tieming/inf553/hw5-py/asnlib/publicdata/generate_stream.jar StreamSimulation ///Users/tieming/inf553/hw5-py/asnlib/publicdata/business.json 9999 100
PORT = 9999
OUTPUT_PATH = "///Users/tieming/inf553/hw5-py/task2.csv"

# PORT = int(sys.argv[1])
# OUTPUT_PATH = sys.argv[2]

# numOfHash would like to be divided by 3
numOfHash = 36

conf = SparkConf().setMaster("local[*]")\
            .setAppName("task2")\
            .set("spark.executor.memory", "4g")\
            .set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", PORT)

# write csv header
csvfile = open(OUTPUT_PATH, 'w')
csv_writer = csv.writer(csvfile, delimiter=',')
csv_writer.writerow(["Time", "Ground Truth", "Estimation"])
csvfile.close()

city_Dstream = lines.window(windowDuration=30, slideDuration=10)\
                .map(lambda line: json.loads(line))\
                .map(lambda line: line["city"])\
                .map(lambda city: hexlify_city(city))\
                .foreachRDD(lambda rdd: flajolet_martin(rdd, OUTPUT_PATH, numOfHash))

ssc.start()
ssc.awaitTermination()





