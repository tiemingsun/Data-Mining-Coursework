import time
import sys
import copy
import json
import random
import csv
import math
import os
import itertools
from collections import defaultdict
from copy import deepcopy
from statistics import mean, median, stdev



class KMean:

    def __init__(self, k, alpha, max_iteration = 30, algorithm="random"):
        self.n_clusters = k
        self._init_cluster_num = alpha * k
        self.max_iteration = max_iteration
        self.algorithm = algorithm

    def fit(self, data_dict):
        '''
        :return centroids: centroids coordinates and their name [ [centroid_index, [coordinates]] , ...]
        :return cluster_res: key -> data index, value -> centroids name
        '''
        
        centroid_list = self.init_centroid(data_dict, algorithm=self.algorithm)
        # print("Duration:", time.time() - start_time)
        
        cluster_prev = defaultdict(list)
        cluster_curr = defaultdict(list)
        

        cur_iteration = 0
        first_time = 1

        while cur_iteration < self.max_iteration and (self.is_change(cluster_prev, cluster_curr)
                                                        or first_time == 1):

            first_time = 0
            cur_iteration += 1
            cluster_prev = deepcopy(cluster_curr)
            cluster_curr = defaultdict(list)
            for index, coordinate in data_dict.items():
                
                global_min = float("inf")
                
                for i, centroid_coordinates in centroid_list:

                    cur_min = self.calcu_euclidean(centroid_coordinates, coordinate)
                    
                    if global_min > cur_min:
                        global_min = cur_min
                        cluster_from = i
                        
                cluster_curr[cluster_from].append(index)
                
            
            # update centroid coordinates -> centroid_list
            for key in cluster_curr.keys():
                centroid_list[key][1] = self.calcu_mean(cluster_curr[key], data_dict)
        
            
        cluster_num_list = []
        for value in cluster_curr.values():
            cluster_num_list.append(len(value))
        
        # print("Total iteration count: ", cur_iteration)
        # print("Total duration: ", time.time() - start_time)
        
        # return cluster_curr, centroid_list, cluster_num_list
        return cluster_curr

            
    def init_centroid(self, data_dict, algorithm="random"):
        '''
        :return centroid_list: [[i, centroid_coordinates], ... ]
        '''
        if len(data_dict) <= self._init_cluster_num:
            centroid_list = []
            init_coordinates = list(data_dict.values())
            for i in range(len(data_dict)):
                centroid_list.append([i, init_coordinates[i]])
                
            return centroid_list
            
        random.seed(17)
        centroid_list = []
        # total_num = len(list(data_dict.keys()))
            
        centroid_index = set()
            
        if algorithm == "random":
            while len(centroid_index) < self._init_cluster_num:
                index = random.choice(set(data_dict.keys()))
                if index not in centroid_index:
                    centroid_index.add(index)
            
            # choose random index in data_dict
            centroid_index = list(centroid_index)
            
            for i in range(len(centroid_index)):

                index = centroid_index[i]
                centroid_list.append( [i, data_dict[str(index)]] )
                
                
        else:
            
            cluster_index_set = set()
            first_index = random.choice(list(data_dict.keys()))
            cluster_index_set.add(str(first_index))
            
            while len(cluster_index_set) < self._init_cluster_num:
                global_max_d = [-float("inf"), "dummy_index"]
                
                for i, _ in data_dict.items():
                    if i not in cluster_index_set:

                        global_d = float("inf")
                        
                        for clus_index in cluster_index_set:
                            local_d = self.calcu_euclidean(data_dict[clus_index],data_dict[i])
                            if global_d > local_d:
                                global_d = local_d
                    else:
                        continue

                    if global_max_d[0] < global_d:
                        global_max_d[0], global_max_d[1] = global_d, i
                        
                cluster_index_set.add(global_max_d[1])
                
            centroid_index = list(cluster_index_set)
            
            for i in range(len(centroid_index)):
                index = centroid_index[i]
                centroid_list.append( [i, data_dict[str(index)]] )
            
        return centroid_list

    
    def calcu_euclidean(self,data_1, data_2):    
        return calcu_euclidean(data_1, data_2)

    
    def is_change(self, cluster_prev, cluster_curr):

        for key in cluster_curr.keys():
            if cluster_prev[key] != cluster_curr[key]:
                return True

        return False
    

    def calcu_mean(self, list_index, data_dict):
        return calcu_mean(list_index, data_dict)



def readfile(path):
    init_data = {}
    with open(path, 'r') as fp:
        data_reader = csv.reader(fp, delimiter=',')
        for row in data_reader:
            init_data[row[0]] = [float(s) for s in row[1:]]
    return init_data


def calcu_euclidean(data_1, data_2):

    distance = 0.0
    if len(data_1) != len(data_2):

        raise("Data must have same dimension!")

    for index in range(len(data_1)):
        distance += (data_1[index] - data_2[index]) ** 2

    return math.sqrt(distance)


def calcu_mean(list_index, data_dict):

    coordinates = []
    for index in list_index:
        coordinates.append(data_dict[index])
    coordinates_transpose = list(map(list, zip(*coordinates)))
    res = []
    for single_list in coordinates_transpose:
        res.append(mean(single_list))

    return res


def calcu_sum(list_index, data_dict):

    coordinates = []
    for index in list_index:
        coordinates.append(data_dict[index])
    coordinates_transpose = list(map(list, zip(*coordinates)))
    res = []
    for single_list in coordinates_transpose:
        res.append(sum(single_list))

    return res


def calcu_mean_sq(list_index, data_dict):

    coordinates = []
    for index in list_index:
        coordinates.append(data_dict[index])
    coordinates_transpose = list(map(list, zip(*coordinates)))
    res = []
    for single_list in coordinates_transpose:
        res.append(sum([c**2 for c in single_list]))

    return res


def generate_stat(cluster_dict, DS_data):
    '''
    :return stat_dict: key -> cluster_index; value -> [N, sum_, sum_SQ]
    '''
    stat_dict = defaultdict(list)
    for key, value in cluster_dict.items():
        
        sum_ = calcu_sum(value, DS_data)
        N = len(value)
        sum_SQ = calcu_mean_sq(value, DS_data)
        
        stat_dict[key] = [N, sum_, sum_SQ]
    
    return stat_dict


def generate_RS_dict(RS_dict, init_data, RS_index):

    for v in RS_index:
        RS_dict[v] = init_data[v]
    return RS_dict


# Generate intermediate result
def write_intermediate(DS_stat, CS_stat, RS_index, INTERMEIDATE_PATH, round_id = 0):
    if round_id == 0:
        
        with open(INTERMEIDATE_PATH, 'w') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',')
            csv_writer.writerow(["round_id", "nof_cluster_discard", "nof_point_discard", "nof_cluster_compression", "nof_point_compression", "nof_point_retained"])
    
    nof_cluster_discard = len(DS_stat)
    nof_cluster_compression = len(CS_stat)
    
    nof_point_discard = 0
    for v, _, _ in DS_stat.values():
        nof_point_discard += v
        
    nof_point_compression = 0
    for v, _, _ in CS_stat.values():
        nof_point_compression += v
    
    nof_point_retained = len(RS_index)
    
    with open(INTERMEIDATE_PATH, 'a') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerow([round_id + 1, nof_cluster_discard, nof_point_discard, nof_cluster_compression, nof_point_compression, nof_point_retained])
        
    return 
    

def calcu_Mahalanobis(cluster_stat, coordinate):
    # yi = (xi - ci) / sigma_i
    global_min = [float("inf"), "dummy_index"]
    for key, value in cluster_stat.items():
        N, sum_, sum_SQ = value[0], value[1], value[2]
        dist = 0.0
        for i in range(len(coordinate)):
            x_i, c_i, sig_i = coordinate[i], sum_[i] / N, math.sqrt(abs(sum_SQ[i] / N - (sum_[i] / N)**2))
            y_i = (x_i - c_i) / sig_i
            
            dist += y_i ** 2
            
        maha_dist = math.sqrt(dist)
            
        if global_min[0] > maha_dist:
            global_min[0], global_min[1] = maha_dist, key    
                        
    return global_min[0], global_min[1]
        

def judge_and_update_cluster(key, coordinate, thres, cluster_stat, cluster_dict_ds, cluster_temp_set):
    maha_dist, clus_index = calcu_Mahalanobis(cluster_stat, coordinate)
    if maha_dist < thres:
        
        cluster_stat[clus_index][0] += 1
        for i in range(len(coordinate)):
            cluster_stat[clus_index][1][i] += coordinate[i]
            cluster_stat[clus_index][2][i] += coordinate[i] ** 2

        # update
        cluster_dict_ds[clus_index].append(key)    
        return 
    else:
        cluster_temp_set.add(key)
        return


def seperate_CS_RS(cluster_dict_RS_and_CS, RS_dict, CS_stat, CS_index, cs_num):
    # update RS_dict and CS_stat, and CS_index
    # cs_num is a global variable!!! 
    for value in cluster_dict_RS_and_CS.values():

        if len(value) > 1:
            cs_n = len(value)
            cs_sum = [0 for _ in range(data_dim)]
            cs_sum_SQ = [0 for _ in range(data_dim)]
            
            for cs_i in value:
                for d in range(data_dim):
                    
                    cs_sum[d] += RS_dict[cs_i][d]
                    cs_sum_SQ[d] += RS_dict[cs_i][d] ** 2
                CS_index[cs_num].append(cs_i)
            CS_stat[cs_num] = [cs_n, cs_sum, cs_sum_SQ]
            
            cs_num += 1
            
            for v in value:
                del RS_dict[v]

    return


def merge_CS(CS_stat, thres, CS_index):
    combine = itertools.combinations(list(CS_stat.keys()), 2)
    
    for i, j in combine:

        if i not in CS_stat.keys() or j not in CS_stat.keys():
            continue

        coord_i = [c / CS_stat[i][0] for c in CS_stat[i][1]]
        
        # calculate Mahalanobis
        N, sum_, sum_SQ = CS_stat[j][0], CS_stat[j][1], CS_stat[j][2]
        # print("I m here")
        # print(N, len(sum_), len(sum_SQ))
        dist = 0.0
        for index_i in range(data_dim):
            # print(i, index_i)
            x_i, c_i, sig_i = coord_i[index_i], sum_[index_i] / N, math.sqrt(abs(sum_SQ[index_i] / N - (sum_[index_i] / N)**2))
            y_i = (x_i - c_i) / sig_i
            
            dist += y_i ** 2
        maha_dist = math.sqrt(dist)
        
        if maha_dist < thres:
            # merge i -> j and delete i
            CS_stat[j][0] += CS_stat[i][0]

            for cs_index_i in range(data_dim):
                CS_stat[j][1][cs_index_i] += CS_stat[i][1][cs_index_i]
                CS_stat[j][2][cs_index_i] += CS_stat[j][1][cs_index_i]
        
            del CS_stat[i]
            
            CS_index[j].extend(CS_index[i])
            del CS_index[i]
    
    return


def merge_all(DS_stat, DS_index, CS_stat, CS_index, RS_dict):
    
    # merge RS
    for key_rs, coordinate in RS_dict.items():
        
        _, clus_index = calcu_Mahalanobis(DS_stat, coordinate)
        DS_index[clus_index].append(key_rs)
    # merge CS
    
    for cs_key, cluster_cs in CS_stat.items():
        # calculate mean coordinate
        coord = [c / cluster_cs[0] for c in cluster_cs[1]]
        
        _, clus_index = calcu_Mahalanobis(DS_stat, coord)
        # update DS cluster
        DS_index[clus_index].extend(CS_index[cs_key])
        
    return


def convert_and_write_file(DS_cluster_index, OUTPUT_PATH):
    output_json = {}
    for key, values in DS_cluster_index.items():
        for v in values:
            output_json[v] = key
    with open(OUTPUT_PATH, 'w') as fp:
        fp.write(json.dumps(output_json))
    
    return


# DIR_PATH = "///Users/tieming/inf553/hw6-py/publicdata/test1/"
# INTERMEIDATE_PATH = "///Users/tieming/inf553/hw6-py/intermediate_1.csv"
# CLUSTER_PATH = "///Users/tieming/inf553/hw6-py/cluster_1.json"
# numOfCluster = 10


DIR_PATH = sys.argv[1]
numOfCluster = int(sys.argv[2])
CLUSTER_PATH = sys.argv[3]
INTERMEIDATE_PATH = sys.argv[4]



start_time = time.time()
path_list = sorted(os.listdir(DIR_PATH))
global cs_num
cs_num = 0
# CS_NUM, RS_NUM = 0, 0
CS_stat = defaultdict(list)
CS_index = defaultdict(list)
DS_stat = defaultdict(list)
RS_dict = defaultdict(list) # key:index, value:coordinates


for cur_index in range(len(path_list)):
    cur_path = DIR_PATH + path_list[cur_index]

    if cur_index == 0:

        init_data_0 = readfile(cur_path)

        # set thres for mahalabous distance
        data_dim = len(init_data_0["0"])
        thres = 2 * math.sqrt(data_dim)

        init_data_train = dict(itertools.islice(deepcopy(init_data_0).items(), 50000))

        print("Initialize KMeans Model ... ")
        # 2.
        mykmeans_init = KMean(k=numOfCluster, alpha=3, max_iteration=5, algorithm="plus")
        cluster_curr_init= mykmeans_init.fit(init_data_train)
        # 3. 
        RS_AND_CS = set()
        for value in cluster_curr_init.values():
            if len(value) == 1:
                RS_AND_CS.add(value[0])

        # init DS dict
        DS_data_2_0 = deepcopy(init_data_0)
        for key in RS_AND_CS:
            del DS_data_2_0[key]

        # init RS + CS dict
        RS_and_CS_dict = {}

        for index in RS_AND_CS:
            RS_and_CS_dict[index] = init_data_0[index]

        mykmean_DS = KMean(k=numOfCluster, alpha=1, max_iteration=100, algorithm="plus")
        DS_index = mykmean_DS.fit(DS_data_2_0)

        # DS stats
        DS_stat = generate_stat(DS_index, DS_data_2_0)

        # run kMean on RS + CS
        mykmean_RS = KMean(k=numOfCluster, alpha=3, max_iteration=5, algorithm="plus")
        cluster_dict_RS_and_CS = mykmean_RS.fit(RS_and_CS_dict)

        # init RS_dict
        for value in cluster_dict_RS_and_CS.values():
            for rs_index in value:
                RS_dict[rs_index] = init_data_0[rs_index]
        
        # update RS_dict and CS_stat, and CS_index
        seperate_CS_RS(cluster_dict_RS_and_CS, RS_dict, CS_stat, CS_index, cs_num)

        write_intermediate(DS_stat, CS_stat, set(RS_dict.keys()), INTERMEIDATE_PATH, cur_index)

        print("BFR model initialized, duration: ", time.time() - start_time)
    
    else:

        init_data = readfile(cur_path)

        cluster_temp_set = set()
        for key,coordinate in init_data.items():
            judge_and_update_cluster(key, coordinate, thres, DS_stat, DS_index, cluster_temp_set)

        # update cluster_temp_set to existing CS cluster
        cluster_temp_rs_set = set()
        for key in cluster_temp_set:
            judge_and_update_cluster(key, init_data[key], thres, CS_stat, CS_index, cluster_temp_rs_set)

        
        # 9. update RS dict and RS set
        RS_dict = generate_RS_dict(RS_dict, init_data, cluster_temp_rs_set)

        mykmean_RS = KMean(k=numOfCluster, alpha=3, max_iteration=5, algorithm="plus")
        cluster_dict_RS_and_CS = mykmean_RS.fit(RS_dict)

        # 10. find cluster number > 1, then move from RS set to CS set
        # update RS_dict and CS_stat and CS_index
        seperate_CS_RS(cluster_dict_RS_and_CS, RS_dict, CS_stat, CS_index, cs_num)

        # 11.
        # print('CS before', CS_index)
        merge_CS(CS_stat, thres, CS_index)
        # print('CS after', CS_index )
        # print(set(RS_dict.keys()))
        # write intermediate
        write_intermediate(DS_stat, CS_stat, set(RS_dict.keys()), INTERMEIDATE_PATH, cur_index)

# merge all -> update to DS_index
merge_all(DS_stat, DS_index, CS_stat, CS_index, RS_dict)
convert_and_write_file(DS_index, CLUSTER_PATH)

print("Duration: ", time.time() - start_time)


