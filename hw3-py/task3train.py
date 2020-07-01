from pyspark import SparkContext
import sys
import json
import math
import time
import random
from itertools import combinations


def generate_index(business_list):
    ans = {}
    for i, business in enumerate(business_list):
        ans[business] = i
    return ans


def convert2dict(tuple_list):
    output = {}
    for single_tuple in tuple_list:
        output[single_tuple[0]] = [0,0]
    for single_tuple in tuple_list:
        output[single_tuple[0]][0] += single_tuple[1]
        output[single_tuple[0]][1] += 1
    for key in output.keys():
        output[key] = output[key][0] / output[key][1]
    return output


def pearson_sim(dict1, dict2):
    dict1_set = set(dict1.keys())
    dict2_set = set(dict2.keys())
    common_set = dict1_set.intersection(dict2_set)
    set_len = len(common_set)
    if set_len < 3:
        return -1
    
    r1_mean, r2_mean = 0, 0
    for key in common_set:
        r1_mean += dict1[key]
        r2_mean += dict2[key]
    r1_mean /= set_len
    r2_mean /= set_len
    
    nominator, denominator= 0, 0
    var1, var2 = 0, 0
    for key in common_set:
        nominator += (dict1[key] - r1_mean) * (dict2[key] - r2_mean)
        var1 += (dict1[key] - r1_mean) ** 2
        var2 += (dict2[key] - r2_mean) ** 2
    denominator = math.sqrt(var1) * math.sqrt(var2)
    if denominator == 0:
        return 0
    ans = nominator / denominator
    return ans


def convert_to_file(res_dict, OUTPUT_PATH):

    with open(OUTPUT_PATH, 'w') as fp:
        for single_dict in res_dict:
            fp.write(json.dumps(single_dict))
            fp.write('\n')
        fp.close()
    return


def generate_hash_list(numOfBusiness, numOfHash):
    '''
    return a list that contains a number of hash functions
    a list of [(a, b) , ... ]
    ''' 
    output_list = []
    for _ in range(numOfHash):
        output_list.append((random.randint(200, numOfBusiness), random.randint(200, numOfBusiness)))
    return output_list


def minhash(hash_list, rdd_list, numOfBusiness):
    res = []
    for hash_a, hash_b in hash_list:
        
        global_min = sys.maxsize - 1
        for index in rdd_list:
            local_min = (hash_a * index + hash_b) % numOfBusiness
            if local_min < global_min:
                global_min = local_min

        res.append(global_min)
    return res


def separate_candidate(bids):
    bids = sorted(bids)
    return combinations(bids, 2)


def my_hash(band_list):
    return sum(band_list)


def LSH(minhash_line, numOfBand):
    '''
    
    :return: list([(band_index, band_slice_hash), uid], ... )
    '''
    uid = minhash_line[0]
    # length of minhash
    len_minhash = len(minhash_line[1])
    res = []
    numOfRow = int(len_minhash / numOfBand)
    for i in range(numOfBand):
        band_slice = minhash_line[1][i * numOfRow: (i+1) * numOfRow]
        band_slice_hash = my_hash(band_slice)
        res.append([(i, band_slice_hash), uid])
    return res


def similarity_from_candidate(pair, user_bids_dict):
    set_1, set_2 = set(user_bids_dict[pair[0]]), set(user_bids_dict[pair[1]])
    intersect = set_1.intersection(set_2)
    
    if len(intersect) < 3:
        return -1
    
    union_length = len(set_1) + len(set_2) - len(intersect)
    jaccard = len(intersect) / union_length
    return jaccard


start_time = time.time()

# TASK_PATH = "///Users/tieming/inf553/hw3-py/data/train_review.json"
# OUTPUT_PATH = "///Users/tieming/inf553/hw3-py/task3user.model"
# MODE = "user_based"

TASK_PATH = sys.argv[1]
OUTPUT_PATH = sys.argv[2]
MODE = sys.argv[3]

sc = SparkContext(master="local", appName="task3")


train_review = sc.textFile(TASK_PATH).map(lambda line: json.loads(line))


business_list = train_review.map(lambda line: line["business_id"]).distinct().collect()
user_list = train_review.map(lambda line: line["user_id"]).distinct().collect()
# create dictionaries b_name -> bid, u_name -> uid, bid -> b_name, uid -> u_name
business_to_id = generate_index(business_list)
user_to_id = generate_index(user_list)
id_to_business = dict((v,k) for k,v in business_to_id.items())
id_to_user = dict((v, k) for k, v in user_to_id.items())


if MODE == "item_based":
    item_cf_rdd = train_review.map(lambda line: (line["business_id"], (line["user_id"], line["stars"])))\
            .map(lambda line: (business_to_id[line[0]], (user_to_id[line[1][0]], line[1][1]) ) )\
            .groupByKey()\
            .filter(lambda x: len(x[1]) >= 3)\
            .mapValues(lambda tuple_list: convert2dict(tuple_list))
    # {bid1:{uid1: stars, uid2: stars, ... }, bid2 ....}
    item_cf_dict = item_cf_rdd.collectAsMap()

    # generate all candidate business pairs, with at least 3 co-rated users
    # set numPartition = 2 -> much faster in setting cartesian
    business_rdd = item_cf_rdd.map(lambda line: line[0]).coalesce(2)
    combined_rdd = business_rdd.cartesian(business_rdd)\
            .filter(lambda pair: pair[0] < pair[1])

    res_dict = combined_rdd.map(lambda pair: (pair, pearson_sim(item_cf_dict[pair[0]], item_cf_dict[pair[1]])) )\
            .filter(lambda x: x[1] > 0)\
            .map(lambda pair: {"b1": id_to_business[pair[0][0]], "b2":id_to_business[pair[0][1]], "sim": pair[1]})\
            .collect()

    convert_to_file(res_dict, OUTPUT_PATH)


    
else:

    numOfUser = len(user_list)
    numOfBusiness = len(business_list)
    numOfHash = 30
    numOfBand = 30
    SIMILARITY = 0.01


    user_bids_rdd = train_review.map(lambda line: (user_to_id[line["user_id"]], business_to_id[line["business_id"]]))\
            .groupByKey()\
            .map(lambda line: (line[0], sorted(list(line[1]))))

    user_bids_dict = user_bids_rdd.collectAsMap()

    hash_list = generate_hash_list(numOfBusiness, numOfHash)

    minhash_rdd = user_bids_rdd.map(lambda line: (line[0], minhash(hash_list, line[1], numOfBusiness)))

    lsh_rdd = minhash_rdd.map(lambda line: LSH(line, numOfBand)).flatMap(lambda x: x)\
            .groupByKey()\
            .map(lambda line: (line[0], sorted(list(line[1]))) )\
            .filter(lambda line: len(line[1]) > 1)\
            .flatMapValues(separate_candidate)\
            .map(lambda line: line[1])\
            .distinct()


    filter_by_jaccard = lsh_rdd.map(lambda pair: (pair, similarity_from_candidate(pair, user_bids_dict)))\
            .filter(lambda line: line[1] >= SIMILARITY)\
            .map(lambda x: x[0])

    user_cf_rdd = train_review.map(lambda line: (line["user_id"], (line["business_id"], line["stars"])))\
            .map(lambda line: (user_to_id[line[0]], (business_to_id[line[1][0]], line[1][1]) ) )\
            .groupByKey()\
            .mapValues(lambda tuple_list: convert2dict(tuple_list))

    # {uid1:{bid1: stars, bid2: stars, ... }, uid2 ....}
    user_cf_dict = user_cf_rdd.collectAsMap()

    user_sim = filter_by_jaccard.map(lambda pair: (pair, pearson_sim(user_cf_dict[pair[0]], user_cf_dict[pair[1]])))\
            .filter(lambda x: x[1] > 0)\
            .map(lambda pair: {"u1": id_to_user[pair[0][0]], "u2":id_to_user[pair[0][1]], "sim": pair[1]})\
            .collect()

    convert_to_file(user_sim, OUTPUT_PATH)


print("Duration is: ", time.time() - start_time)