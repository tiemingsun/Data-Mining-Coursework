from pyspark import SparkContext
import sys
import json
import random
from itertools import combinations
import time


def generate_hash_list(numOfUser, numOfHash):
    '''
    return a list that contains a number of hash functions
    ''' 
    output_list = []
    for _ in range(numOfHash):
        output_list.append((random.randint(200, numOfUser), random.randint(200, numOfUser)))
    return output_list


def minhash(hash_list, rdd_list, numOfUser):
    res = []
    for hash_a, hash_b in hash_list:
        
        global_min = sys.maxsize - 1
        for index in rdd_list:
            local_min = (hash_a * index + hash_b) % numOfUser
            if local_min < global_min:
                global_min = local_min

        res.append(global_min)
    return res


def LSH(minhash_line, numOfBand):
    bid = minhash_line[0]
    # length of minhash
    len_minhash = len(minhash_line[1])
    res = []
    numOfRow = int(len_minhash / numOfBand)
    for i in range(numOfBand):
        band_slice = minhash_line[1][i * numOfRow: (i+1) * numOfRow]
        band_slice_hash = my_hash(band_slice)
        res.append([(i, band_slice_hash), bid])
    return res


def separate_candidate(bids):
    bids = sorted(bids)
    return combinations(bids, 2)


def my_hash(band_list):
    return sum(band_list)


def similarity_from_candidate(pair, business_user_dict):
    set_1, set_2 = set(business_user_dict[pair[0]]), set(business_user_dict[pair[1]])
    intersect = set_1.intersection(set_2)
    union_length = len(set_1) + len(set_2) - len(intersect)
    jaccard = len(intersect) / union_length
    return jaccard


def write_output(similar_dict, OUTPUT_PATH):
    with open(OUTPUT_PATH, 'w') as output_file:
        for single_dict in similar_dict:
            output_file.write(json.dumps(single_dict))
            output_file.write('\n')
    return   


if __name__ == "__main__":
    
    # TASK_1_PATH = "///Users/tieming/inf553/hw3-py/data/train_review.json"
    # OUTPUT_PATH = "///Users/tieming/inf553/hw3-py/task1.res"
    # spark-submit task1.py ///Users/tieming/inf553/hw3-py/data/train_review.json ///Users/tieming/inf553/hw3-py/task1.res
    TASK_1_PATH = sys.argv[1]
    OUTPUT_PATH = sys.argv[2]
    numOfHash = 35
    numOfBand = 35
    SIMILARITY = 0.05

    start_time = time.time()


    sc = SparkContext(master="local", appName="task1")
    rdd_1 = sc.textFile(TASK_1_PATH)
    rdd_1 = rdd_1.map(lambda line: json.loads(line))

    train_review = rdd_1.map(lambda line: (line["business_id"], line["user_id"]))

    user_id_list = rdd_1.map(lambda line: line["user_id"]).distinct().collect()

    numOfUser = len(user_id_list)

    user_index_dict = {}
    for i ,user in enumerate(user_id_list):
        user_index_dict[user] = i

    numOfBusiness = rdd_1.map(lambda line: line["business_id"]).distinct().count()

    business_user_rdd = train_review.map(lambda line: (line[0], user_index_dict[line[1]]) ).groupByKey()\
                .map(lambda line: (line[0], sorted(list(line[1]))))

    business_user_dict = business_user_rdd.collectAsMap()
    hash_list = generate_hash_list(numOfUser, numOfHash)

    minhash_rdd = business_user_rdd.map(lambda line: (line[0], minhash(hash_list, line[1], numOfUser)))


    lsh_rdd = minhash_rdd.map(lambda line: LSH(line, numOfBand)).flatMap(lambda x: x)\
                    .groupByKey()\
                    .map(lambda line: (line[0], list(line[1])) )\
                    .filter(lambda line: len(line[1]) > 1)\
                    .flatMapValues(separate_candidate)\
                    .map(lambda line: line[1])\
                    .distinct().persist()

    similar_dict = lsh_rdd.map(lambda line: (line, similarity_from_candidate(line, business_user_dict)) )\
                    .filter(lambda line: line[1] >= SIMILARITY)\
                    .map(lambda line: {"b1":line[0][0], "b2":line[0][1], "sim":line[1]} )\
                    .collect()
    print("Accuracy for LSH is: " ,len(similar_dict) / 59435)

    write_output(similar_dict, OUTPUT_PATH)

    print("Duration: ", time.time() - start_time)
