from pyspark import SparkContext
import sys
import json
import math
import time


def import_model(MODEL_PATH):

    with open(MODEL_PATH, 'r') as fp:
        fp.readline()
        trained_business = json.loads(fp.readline())
        fp.readline()
        trained_user = json.loads(fp.readline())

    for key in trained_business.keys():
        word_set = set(trained_business[key])
        trained_business[key] = word_set
    for key in trained_user.keys():
        business_set = set(trained_user[key])
        trained_user[key] = business_set

    return trained_business, trained_user


def cosine_distance(user, business, trained_business, trained_user):
    
    if user not in trained_user.keys() or business not in trained_business.keys():
        return 0
    business_set = trained_user[user]
    user_word = set()
    for single_business in business_set:
        user_word.update(trained_business[single_business])
    word_len = len(user_word)

    intersect_len = trained_business[business].intersection(user_word)
    
    cosine = len(intersect_len) / (math.sqrt(word_len) * math.sqrt(len(trained_business[business])))
    return cosine


def convert_to_file(res_sim, OUTPUT_PATH):
    res_list = []
    for line in res_sim:
        single_dict = {"user_id": line[0][0], "business_id": line[0][1], "sim": line[1]}
        res_list.append(single_dict)
    with open(OUTPUT_PATH, 'w') as fp:
        for single_dict in res_list:
            fp.write(json.dumps(single_dict))
            fp.write('\n')
        fp.close()
    return


if __name__ == "__main__":
    
    # MODEL_PATH = "///Users/tieming/inf553/hw3-py/task2.model"
    # OUTPUT_PATH = "///Users/tieming/inf553/hw3-py/task2.predict"
    # TEST_PATH = "///Users/tieming/inf553/hw3-py/data/test_review.json"
    # spark-submit task2predict.py ///Users/tieming/inf553/hw3-py/data/test_review.json ///Users/tieming/inf553/hw3-py/task2.model ///Users/tieming/inf553/hw3-py/task2.predict
    TEST_PATH = sys.argv[1]
    MODEL_PATH = sys.argv[2]
    OUTPUT_PATH = sys.argv[3]

    start_time = time.time()
    sc = SparkContext(master="local", appName="task2predict")

    trained_business, trained_user = import_model(MODEL_PATH)

    test_rdd = sc.textFile(TEST_PATH)
    test_rdd = test_rdd.map(lambda line: json.loads(line))\
                .map(lambda line: (line["user_id"], line["business_id"]))

    cosine_sim = test_rdd.map(lambda line: ( (line[0], line[1]), cosine_distance(line[0], line[1], trained_business, trained_user)) )\
                        .filter(lambda line: line[1] >= 0.01)

    res_sim = cosine_sim.collect()
    convert_to_file(res_sim, OUTPUT_PATH)

    print("Duration: ", time.time() - start_time)