from pyspark import SparkContext
import sys
import json
from collections import defaultdict
import math
import time


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


def standardlize_value(list_of_list):
    output_list = []
    for single_list in list_of_list:
        if single_list != None:
            for sim_pair in single_list:
                if sim_pair != None:
                    output_list.append(sim_pair)
    output_dict = {k:v for k, v in output_list}
    
    return output_dict


def pred_item_based(user_id,business_id,model_dict,user_dict,N):
    # model_dict[user_id] -> find ( top N && in user_dict[user_id].keys() )
    if user_id not in user_dict.keys() or business_id not in model_dict.keys():
        return -1
    
    neighbor = []
    for business in user_dict[user_id].keys():
        if business in model_dict[business_id].keys():
            neighbor.append((business, model_dict[business_id][business]))
    # choose top N in neighbor
    if len(neighbor) == 0:
        return -1
    
    if len(neighbor) > N:
        topN = sorted(neighbor, key=lambda x: [x[1], x[0]], reverse=True)[:N]
    else:
        topN = neighbor
        
    nominator, denominator = 0, 0
    for business, weight in topN:
        nominator += user_dict[user_id][business] * weight
        denominator += weight
    
    pred = nominator / denominator
    
    return pred

def pred_user_based(user_id,business_id,model_dict,business_dict,stars_for_user,N):
    if business_id not in business_dict.keys() or user_id not in model_dict.keys():
        return -1
    
    neighbor = []
    for user in business_dict[business_id].keys():
        if user in model_dict[user_id].keys():
            neighbor.append((user, model_dict[user_id][user]))
    # choose top N in neighbor
    if len(neighbor) == 0:
        return -1

    user_avg = sum(stars_for_user[user_id][0]) / stars_for_user[user_id][1]

    if len(neighbor) > N:
        # topN: [(user_id, weight), ...]
        topN = sorted(neighbor, key=lambda x: [x[1], x[0]], reverse=True)[:N]
    else:
        topN = neighbor

    nominator, denominator = 0, 0
    for user, weight in topN:
        # average rating for all other businesses
        neighbor_user_avg = (sum(stars_for_user[user][0]) - business_dict[business_id][user]) / (stars_for_user[user][1] - 1)
        nominator += (business_dict[business_id][user] - neighbor_user_avg)* weight
        denominator += weight
    
    pred = user_avg + nominator / denominator
    return pred

def convert_to_file(res_dict, OUTPUT_PATH):

    with open(OUTPUT_PATH, 'w') as fp:
        for single_dict in res_dict:
            fp.write(json.dumps(single_dict))
            fp.write('\n')
        fp.close()
    return



MODEL_PATH = "///Users/tieming/inf553/hw3-py/task3user.model"
TRAIN_PATH = "///Users/tieming/inf553/hw3-py/data/train_review.json"
TEST_PATH = "///Users/tieming/inf553/hw3-py/data/test_review.json"
OUTPUT_PATH = "///Users/tieming/inf553/hw3-py/task3user.predict"
MODE = "user_based"

# MODEL_PATH = sys.argv[3]
# TRAIN_PATH = sys.argv[1]
# TEST_PATH = sys.argv[2]
# OUTPUT_PATH = sys.argv[4]
# MODE = sys.argv[5]

sc = SparkContext(master="local", appName="task3")

start_time = time.time()


train_review = sc.textFile(TRAIN_PATH).map(lambda line: json.loads(line))
test_review = sc.textFile(TEST_PATH).map(lambda line: json.loads(line))
moedel_raw = sc.textFile(MODEL_PATH).map(lambda line: json.loads(line))

if MODE == "item_based":
    item_cf_rdd = train_review.map(lambda line: [line["user_id"], (line["business_id"], line["stars"])] )\
                    .groupByKey()\
                    .mapValues(lambda tuple_list: convert2dict(tuple_list))
    # user_dict: {uid: {bid1: star, ...}, ...}
    user_dict = item_cf_rdd.collectAsMap()


    model_rdd_b1 = moedel_raw.map(lambda line: [line["b1"], (line["b2"], line["sim"])]).groupByKey()
    model_rdd_b2 = moedel_raw.map(lambda line: [line["b2"], (line["b1"], line["sim"])]).groupByKey()
    model_rdd = model_rdd_b1.fullOuterJoin(model_rdd_b2)\
                    .map(lambda line: (line[0], standardlize_value(line[1])))
    # model_dict: {bid: {bid1: sim, ...}, ...}
    model_dict = model_rdd.collectAsMap()

    test_rdd = test_review.map(lambda line: ((line["user_id"], line["business_id"]), pred_item_based(line["user_id"], line["business_id"],model_dict,user_dict,7 )))\
                        .filter(lambda x: x[1] > 0)\
                        .map(lambda line: {"user_id": line[0][0], "business_id": line[0][1], "stars": line[1]})


else:
    business_cf_rdd = train_review.map(lambda line: [line["business_id"], (line["user_id"], line["stars"])])\
                    .groupByKey()\
                    .mapValues(lambda tuple_list: convert2dict(tuple_list))
    # business_dict: {bid: {uid1: star, ...}, ...}
    business_dict = business_cf_rdd.collectAsMap()
    
    stars_for_user = train_review.map(lambda line: (line["user_id"], line["stars"]))\
                    .groupByKey()\
                    .map(lambda line: (line[0], [list(line[1]), len(line[1])] ))\
                    .collectAsMap()


    model_rdd_b1 = moedel_raw.map(lambda line: [line["u1"], (line["u2"], line["sim"])]).groupByKey()
    model_rdd_b2 = moedel_raw.map(lambda line: [line["u2"], (line["u1"], line["sim"])]).groupByKey()
    model_rdd = model_rdd_b1.fullOuterJoin(model_rdd_b2)\
                    .map(lambda line: (line[0], standardlize_value(line[1])))

    # model_dict: {uid: {uid1: sim, ...}, ...}
    model_dict = model_rdd.collectAsMap()

    test_rdd = test_review.map(lambda line: ((line["user_id"], line["business_id"]), pred_user_based(line["user_id"], line["business_id"],model_dict,business_dict, stars_for_user,7 )))\
                        .filter(lambda x: x[1] > 0)\
                        .map(lambda line: {"user_id": line[0][0], "business_id": line[0][1], "stars": line[1]})



convert_to_file(test_rdd.collect(), OUTPUT_PATH)

print("Duration is: ", time.time() - start_time)
    