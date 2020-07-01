from pyspark import SparkContext
import csv
import sys
from collections import Counter
from itertools import combinations
from operator import add
import math
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


def generate_basket(case, line):
    if case == 2:
        line[0], line[1] = line[1], line[0]
    return line

def generate_candidate(item_num, freq_item):

    candidate_dict = {}
    for tup_1 in freq_item:
        for tup_2 in freq_item:
            temp = set(tup_1[0] + tup_2[0])
            if len(temp) == item_num:
                candidate_dict[tuple(temp)] = 0
    return candidate_dict


def pcy_hash(pair, bitmap_num):
    return int(pair[0] + pair[1]) % bitmap_num


def filter_tuples(chunk, candidate_list, item_num, bitmap_num, chunk_s, freq_item, single_item, list_of_set):
    
    if item_num == 2:
        
        candidate_pair_dict = {}
        bitmap = [0 for _ in range(bitmap_num)]
        for single_list in chunk:
            if len(single_list[1]) >= 2:
                for pair in combinations(single_list[1], 2):
                    candidate_pair_dict[pair] = 0
                    bitmap[pcy_hash(pair, bitmap_num)] += 1
        filter_pair = []
        for i in range(len(bitmap)):
            if bitmap[i] < chunk_s:
                filter_pair.append(i) 


        for pair in candidate_pair_dict.keys():
            if pcy_hash(pair, bitmap_num) in filter_pair:
                continue
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



def find_freq_pcy(chunk, numOfBasket, s, bitmap_num, candidate_list):
    
    chunk_s = int(s * len(chunk) / numOfBasket)
    single_items = []
    for single_list in chunk:
        for item in single_list[1]:
            single_items.append(item)
    
    # set a counter for single items
    item_counter = Counter(single_items)
    freq_item = set()
    for item in item_counter:
        if item_counter[item] > chunk_s:
            freq_item.add( ((item, ), 1) )
    candidate_list.append(list(freq_item))
        
    item_num = 1
    single_item = freq_item

    list_of_set = []
    for single_list in chunk:
        list_of_set.append(set(single_list[1]))
    
    while len(freq_item) > 0:
        item_num += 1
        freq_item = filter_tuples(chunk, candidate_list, item_num, bitmap_num, chunk_s, freq_item, single_item, list_of_set)
        
        if len(freq_item) > 0:
            candidate_list.append(list(freq_item)  ) 
            
    return candidate_list


def map_func(line):
    return sum(map(lambda x: int(x), list(line)))


def son_filter(c_itemset, basket_list):
    support = 0
    for single_line in basket_list:
        if type(c_itemset[0]) == int:
            if c_itemset[0] in single_line:
                support += 1
                
        elif all(elem in single_line for elem in c_itemset[0]):
            support += 1
            
    return (c_itemset[0], support)


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
    # TEST_1_PATH = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/small1.csv"
    # output_file_path = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/output"
    # case_num = 1
    # s = 5
    # spark-submit task1.py 2 9 ///Users/tieming/inf553/hw2-py/asnlib/publicdata/small2.csv ///Users/tieming/inf553/hw2-py/asnlib/publicdata/output

    num_partition = 3
    bitmap_num = 2
    start_time = time.time()
    case_num = int(sys.argv[1])
    s = int(sys.argv[2])
    test_1 = readfile(sys.argv[3])
    output_file_path = sys.argv[4]
    # test_1 = readfile(TEST_1_PATH)


    sc = SparkContext(master="local", appName="task1")

    rdd_1 = sc.parallelize(test_1)

    basket_rdd = rdd_1.map(lambda line: generate_basket(case_num, line)).groupByKey().mapValues(set).mapValues(list).map(lambda x: x[1])\
                      .map(lambda x: sorted(x)).persist()

    numOfBasket = len(basket_rdd.collect())
    basket_list = basket_rdd.collect()

    map1 = basket_rdd.map(lambda line: (map_func(line), line)).partitionBy(num_partition)\
                     .mapPartitions(lambda chunk: find_freq_pcy(list(chunk), numOfBasket, s, bitmap_num, []) )\
                     .flatMap(lambda x: x)

    reduce1 = map1.reduceByKey(add).persist()

    map2 = reduce1.map(lambda x: son_filter(x, basket_list)).persist()
    reduce2 = map2.filter(lambda line: line[1] >= s)

    output_1 = change_into_str(gather_same_size(reduce1.collect()))
    output_2 = change_into_str(gather_same_size(reduce2.collect()))

    with open(output_file_path, 'w') as file:
        file.write("Candidates:\n" + output_1 + "Frequent Itemsets:\n" + output_2)

    finish_time = time.time()

    print("Duration: %.2f" %(finish_time - start_time))
