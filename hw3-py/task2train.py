from pyspark import SparkContext
import sys
import json
import itertools
import re
from collections import defaultdict
from operator import itemgetter, add
import math
import time


def read_stop_words(STOP_WORDS):
    with open(STOP_WORDS, 'r') as rf:
        stopwords = set(word.strip() for word in rf)
    return stopwords


def review_parser(text, stopwords_set):
    text = text.strip()
    pattern = re.compile(r"[\s\n\d\$\%\(\[\,\.\!\?\:\;\]\)\'\-\"\~\\\*]")

    text_list_raw = re.split(pattern, text)

    text_list = []
    for word in text_list_raw:
        if word.lower() not in stopwords_set and word.lower()!="":
            text_list.append(word.lower())
    return text_list


def calculate_tf(list_of_list):
    single_list = list(itertools.chain.from_iterable(list_of_list))
    tf_dict = defaultdict(float)
    for word in single_list:
        tf_dict[word] += 1
    max_item = max(tf_dict.items(), key=itemgetter(1))[1]
    
    for word in tf_dict.keys():
        tf_dict[word]/= max_item
    return tf_dict


def tf_idf_calculator(idf_dict, tf_dict):
    for item in tf_dict.keys():
        tf_dict[item] *= idf_dict[item]
    
    return sorted(tf_dict.items(), key=lambda x: [-x[1], x[0]])[:200]


def merge_words(list_of_list):
    res = []
    for single_list in list_of_list:
        res.append(single_list[0])
    return res


def export_file(business_profile, user_profile, OUTPUT_TRAIN):
    with open(OUTPUT_TRAIN, 'w') as file:
        file.write("Business:\n" + json.dumps(business_profile) +"\n" + "Users:\n" + json.dumps(user_profile))
    file.close()
    return 


if __name__ == "__main__":

    start_time = time.time()

    # TASK_PATH = "///Users/tieming/inf553/hw3-py/data/train_review.json"
    # STOP_WORDS = "///Users/tieming/inf553/hw3-py/data/stopwords"
    # OUTPUT_TRAIN = "///Users/tieming/inf553/hw3-py/task2.model"
    # spark-submit task2train.py ///Users/tieming/inf553/hw3-py/data/train_review.json ///Users/tieming/inf553/hw3-py/task2.model ///Users/tieming/inf553/hw3-py/data/stopwords
    TASK_PATH = sys.argv[1]
    OUTPUT_TRAIN = sys.argv[2]
    STOP_WORDS = sys.argv[3]

    stopwords_set = read_stop_words(STOP_WORDS)

    sc = SparkContext(master="local", appName="task2train")

    rdd_train = sc.textFile(TASK_PATH)
    rdd_train = rdd_train.map(lambda line: json.loads(line))
    numOfBusiness = rdd_train.map(lambda line: line["business_id"]).distinct().count()

    rdd_parse_review = rdd_train.map(lambda line: (line["business_id"], review_parser(line["text"],stopwords_set)))   
    business_rdd = rdd_parse_review.groupByKey().persist()

    tf_count_rdd = business_rdd.map(lambda line: (line[0], calculate_tf(list(line[1])) ))

    idf_count_rdd = tf_count_rdd.map(lambda line: (line[0], line[1].items())).flatMapValues(lambda x:x)\
                            .map(lambda line: (line[1][0], 1))\
                            .reduceByKey(add).map(lambda line: (line[0], math.log(numOfBusiness/line[1], 2)) )
    
    idf_dict = idf_count_rdd.collectAsMap()

    tf_idf_rdd = tf_count_rdd.map(lambda line: (line[0], tf_idf_calculator(idf_dict, line[1])))

    business_profile = tf_idf_rdd.map(lambda line: (line[0], merge_words(line[1]) ) ).collectAsMap()

    user_profile = rdd_train.map(lambda line: (line["user_id"], line["business_id"]))\
                    .groupByKey().collectAsMap()
    for item in user_profile.keys():
        user_profile[item] = list(user_profile[item])


    export_file(business_profile, user_profile, OUTPUT_TRAIN)
    print("Duration: ", time.time() - start_time)