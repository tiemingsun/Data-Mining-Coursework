from pyspark import SparkContext
import csv
import sys
from collections import Counter
from itertools import combinations
from operator import add
import math
import json
import time


def readfile(TEST_PATH):
    with open(TEST_PATH) as file:
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


def filter_tuples_apriori(chunk, item_num, chunk_s, freq_item, single_item, list_of_set):
    
    if item_num == 2:
        
        candidate_pair_dict = generate_pair(single_item)

        print("pair generated, size", len(candidate_pair_dict))
        for pair in candidate_pair_dict.keys():
            for single_set in list_of_set:
                if set(pair).issubset(single_set) and candidate_pair_dict[pair] < chunk_s:
                    candidate_pair_dict[pair] += 1
                    
        pair_set = set()      
        for pair in candidate_pair_dict.keys():
            if candidate_pair_dict[pair] == chunk_s:
                pair_set.add((pair, 1)) 
        print("final pair length is ", len(pair_set))
        return pair_set
        
    else:
        
        candidate_dict = generate_candidate(item_num, freq_item)
        
        print("triple generated, size", len(candidate_dict))
        for multi in candidate_dict.keys():
            for single_set in list_of_set:
                if set(multi).issubset(single_set) and candidate_dict[multi] < chunk_s:
                    candidate_dict[multi] += 1

        multi_set = set()
        
        for multi in candidate_dict.keys():
            if candidate_dict[multi] == chunk_s:
                multi_set.add((multi, 1))
        print("final set length", len(multi_set))
        return multi_set


def generate_pair(single_item):

    candidate_dict = {}
    for single_1 in single_item:
        for single_2 in single_item:
            if single_1[0] != single_2[0]:
                sort_tup = tuple(sorted(list(single_1[0] + single_2[0])))
                # print(sort_tup)
                candidate_dict[sort_tup] = 0
    return candidate_dict


def generate_candidate(item_num, freq_item):

    candidate_dict = {}
    for tup_1 in freq_item:
        for tup_2 in freq_item:
            temp = set(tup_1[0] + tup_2[0])
            if len(temp) == item_num:
                candidate_dict[tuple(temp)] = 0
    return candidate_dict


def find_freq_apriori(chunk, numOfBasket, s, candidate_list):
    
    chunk_s = s * len(chunk) / numOfBasket
    chunk_s = int(chunk_s)
    print("s for this chunk is", chunk_s)
    single_items = []
    for single_list in chunk:
        for item in single_list[1]:
            single_items.append(item)
    
    # set a counter for single items
    item_counter = Counter(single_items)
    freq_item = set()
    for item in item_counter:
        if item_counter[item] >= chunk_s:
            freq_item.add( ((item,), 1) )
    candidate_list.append(list(freq_item))
    item_num = 1
    single_item = freq_item
    print("single item numbers",len(single_item))
    
    list_of_set = []
    for single_list in chunk:
        list_of_set.append(set(single_list[1]))
            
    while len(freq_item) > 0:
        item_num += 1
        freq_item = filter_tuples_apriori(chunk, item_num, chunk_s, freq_item, single_item, list_of_set)
        print("Number of itemset with length {} : {}".format(item_num, len(freq_item)) )
        if len(freq_item) > 0:
            candidate_list.append(list(freq_item)  ) 
            
    return candidate_list


def map_func(line):
    ans = 0 
    for b_id in list(line):
        ans += sum(ord(ch) for ch in b_id)
    return ans


def son_filter(c_itemset, list_of_set):
    support = 0
    for single_set in list_of_set:
        if set(c_itemset[0]).issubset(single_set):
            support += 1
            
    return (list(set(c_itemset[0])), support)


def generate_set(basket_rdd):
    basket_list = basket_rdd.collect()
    list_of_set = []
    for line in basket_list:
        list_of_set.append(set(line))
    return list_of_set


def gather_same_size(reduce_list):
    if len(reduce_list) == 0:
        return []
    reduce_list = sorted(reduce_list, key=lambda x: [len(x[0]), x[0]])
    max_tuple_size = len(reduce_list[-1][0])
    res = [[] for _ in range(max_tuple_size)]
    for i in reduce_list:
        res[len(i[0])-1].append(i[0])
    
    return res


def change_into_str(list_of_list):
    
    for i in range(len(list_of_list)):
        if i == 0:
                
            list_of_list[i] = list(map(lambda x: '(' + '\'' + "".join(x) + '\'' + ')', iter(list_of_list[i])) )
        else:
            list_of_list[i] = list(map(lambda x: str(tuple(sorted(x))), iter(list_of_list[i])) )
    string = "" 
    for i in range(len(list_of_list)):
        string += ",".join(list_of_list[i])
        string += "\n\n"
    return string



if __name__ == "__main__":
    # YELP_PATH = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/preprocessing.csv"
    # OUTPUT_FILE = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/output_task_2"
    # s = 50
    # user_threshold = 70

    # spark-submit task2.py 70 50 ///Users/tieming/inf553/hw2-py/asnlib/publicdata/preprocessing.csv ///Users/tieming/inf553/hw2-py/asnlib/publicdata/output_task_2

    num_partition = 4
    start_time = time.time()
    
    user_threshold = int(sys.argv[1])
    s = int(sys.argv[2])
    YELP_PATH = sys.argv[3]
    OUTPUT_FILE = sys.argv[4]



    sc = SparkContext(master="local", appName="task2")

    yelp_data = readfile(YELP_PATH)
    rdd_1 = sc.parallelize(yelp_data)
    
    basket_rdd = rdd_1.groupByKey().mapValues(set).filter(lambda line: len(line[1]) > user_threshold)\
                    .map(lambda x: x[1])\
                    .map(lambda x: sorted(list(x))).persist()


    numOfBasket = len(basket_rdd.collect())

    map1 = basket_rdd.map(lambda line: (map_func(line), line)).partitionBy(num_partition)\
          .mapPartitions(lambda chunk: find_freq_apriori(list(chunk), numOfBasket, s,[]))\
          .flatMap(lambda x: list(set(x)))
    reduce1 = map1.reduceByKey(add).persist()

    list_of_set = generate_set(basket_rdd)

    map2 = reduce1.map(lambda x: son_filter(x, list_of_set)).persist()
    reduce2 = map2.filter(lambda line: line[1] >= s)

    output_1 = change_into_str(gather_same_size(reduce1.collect()))
    output_2 = change_into_str(gather_same_size(reduce2.collect()))

    with open(OUTPUT_FILE, 'w') as file:
        file.write("Candidates:\n" + output_1 + "Frequent Itemsets:\n" + output_2)

    finish_time = time.time()

    print("Duration: %.2f" %(finish_time - start_time))