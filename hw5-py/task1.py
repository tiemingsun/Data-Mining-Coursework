import time
import sys
import copy
import json
import random
from binascii import hexlify
from math import exp


def read_file(DATA_PATH):
    
    data_output = []
    with open(DATA_PATH, 'r') as fp:
        
        line = fp.readline()
        while line:
            cur_line = json.loads(line)
            
            if cur_line["city"]:
                city_code = int(hexlify(cur_line["city"].encode('utf8')),16)
            else:
                city_code = int(hexlify("a".encode('utf8')),16)
            data_output.append(city_code)
            
            line = fp.readline()
            
    return data_output


def estimate_error_rate(HASH_FUNC_NUM, BIT_NUM, INSERTED_NUM):
    k = HASH_FUNC_NUM
    n = BIT_NUM
    m = INSERTED_NUM
    return (1 - exp(-k * m / n)) ** k


def estimate_f(HASH_FUNC_NUM, BIT_NUM, INSERTED_NUM):
    k = HASH_FUNC_NUM
    n = BIT_NUM
    m = INSERTED_NUM
    return k * m / n


def generate_hash_list(numOfSet, numOfHash):
    '''
    return a list that contains a number of hash functions
    ''' 
    output_list = []
    for _ in range(numOfHash):
        output_list.append((random.randint(200, numOfSet), random.randint(200, numOfSet)))
    return output_list


def train_list(hash_params, city_distinct, BIT_NUM):
    '''
    :return bit_list: a list with 0 / 1, trained by a set of hash functions and data
    '''
    bit_list = [0 for _ in range(BIT_NUM)]
    one_in_list = 0
    for city in city_distinct:
        
        for hash_a, hash_b in hash_params:
            cur_index = (hash_a * city + hash_b) % BIT_NUM
            bit_list[cur_index] = 1
            one_in_list += 1
    print("Number of 1 in list: ", one_in_list)
    return bit_list


def predict_list(hash_params, city_second, bit_list):
    '''
    : return output_list: 1 - city has existed before, 0 - city hasn't been explored
    '''
    output_list = []
    
    zero_in_list = 0
    
    for city in city_second:
        
        output_list.append("1")
        
        for hash_a, hash_b in hash_params:
            cur_index = (hash_a * city + hash_b) % BIT_NUM
            
            if bit_list[cur_index] == 0:
                output_list[-1] = "0"
                zero_in_list += 1
                break
                
        
    print("Number of Zero in list: ", zero_in_list)
    return output_list


def convert_to_file(output_list, OUTPUT_PATH):
    output_str = " ".join(output_list)
    with open(OUTPUT_PATH, 'w') as fp:
        fp.write(output_str)
    fp.close()
    return



DATA_FIRST = "///Users/tieming/inf553/hw5-py/asnlib/publicdata/business_first.json"
DATA_SECOND = "///Users/tieming/inf553/hw5-py/asnlib/publicdata/business_second.json"
OUTPUT_PATH = "///Users/tieming/inf553/hw5-py/task1"

# DATA_FIRST = sys.argv[1]
# DATA_SECOND = sys.argv[2]
# OUTPUT_PATH = sys.argv[3]

city_first = read_file(DATA_FIRST)
city_second = read_file(DATA_SECOND)
city_distinct = list(set(city_first))

HASH_FUNC_NUM = 10
BIT_NUM = 10000
INSERTED_NUM = len(city_distinct)

print("Estimate Error Rate is:", estimate_error_rate(HASH_FUNC_NUM, BIT_NUM, INSERTED_NUM))
print("Estimate f is:", estimate_f(HASH_FUNC_NUM, BIT_NUM, INSERTED_NUM))

hash_params = generate_hash_list(INSERTED_NUM, HASH_FUNC_NUM)

bit_list = train_list(hash_params, city_distinct, BIT_NUM)

output_list = predict_list(hash_params, city_second, bit_list)

convert_to_file(output_list, OUTPUT_PATH)
