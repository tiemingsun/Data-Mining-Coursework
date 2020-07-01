from pyspark import SparkContext,SparkConf
import os
import csv
import time
import sys
from collections import deque,defaultdict
from operator import add
import time
import copy



def readfile(DATA_FILE):
    with open(DATA_FILE, 'r') as file:
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


def find_relation(src, dst, id_dict, THRESHOLD):
    set1, set2 = id_dict[src], id_dict[dst]
    len_intersect = len(set1.intersection(set2))
    if len_intersect >= THRESHOLD:
        return "follow"
    else:
        return "not follow"


def level_order(root, edge_map):
    '''
    :param root: str bid
    :param edge_map: dict, the default edge adjacent matrix
    :return res_dict: dict {level_num: {bid, bid...}}
    '''
    path_num = defaultdict(float)
    direct_from = defaultdict(str)
    node_level = defaultdict(int)
    
    queue = deque()
    visited = set()
    visited.add(root)
    queue.append(root)
    
    
    res_dict = defaultdict(set)
    cur_level = 0
    res_dict[cur_level].add(root)
    # total_num = 1
    
    node_level[root] = cur_level
    path_num[root] = 1
    direct_from[root] = "I'm root"
    
    
    while len(queue):
        
        this_level = len(queue)
        cur_level += 1
        
        for _ in range(this_level):
            
            head = queue.popleft()
            for next_level_node in edge_map[head]:
                
                if next_level_node not in visited:
                    queue.append(next_level_node)
                    visited.add(next_level_node)
                    
                    res_dict[cur_level].add(next_level_node)
                    
                    node_level[next_level_node] = cur_level
                    path_num[next_level_node] = path_num[head]
                    direct_from[next_level_node] = head
                
                elif next_level_node in visited and node_level[next_level_node] == cur_level and direct_from[next_level_node] != head:
                    path_num[next_level_node] += path_num[head]
                    
                    
        # total_num += len(res_dict[cur_level])
        
    return res_dict, path_num


def create_node_credit(user_list):
    
    user_init_credit = defaultdict(float)
    for user in user_list:
        user_init_credit[user] = 1.0
    return user_init_credit


def create_edge_credit(res_dict, edge_map, user_list, path_num):
    
    user_credit = create_node_credit(user_list)
    
    edge_list = []
    
    cur_level = max(res_dict.keys())
    while cur_level > 0:
        for node_lower in res_dict[cur_level]:
            node_set = edge_map[node_lower].intersection(res_dict[cur_level-1])
            # set_size = len(node_set)

            for node_upper in node_set:

                # assign credit to edge
                node_pair = sorted([node_upper, node_lower])
                edge_score = user_credit[node_lower] * (path_num[node_upper] / path_num[node_lower])

                # assign credit to upper level node
                user_credit[node_upper] += edge_score
                
                edge_list.append([tuple(node_pair), edge_score])
        cur_level -= 1
    return edge_list





def delete_max_edges(cur_edge_map, top_betweenness_pair):
    
    cur_edge_map[top_betweenness_pair[0]].remove(top_betweenness_pair[1])
    cur_edge_map[top_betweenness_pair[1]].remove(top_betweenness_pair[0])
    
    return


def community_generator(user_list, cur_edge_map):
    '''
    :param user_list: distinct user list
    :param cur_edge_map: modified edge map
    :return res_community: List(List())
    '''
    res_community = []
    node_visited = []
    for user in user_list:
        
        if user not in node_visited:
            queue = deque()
            visited = set()

            visited.add(user)
            queue.append(user)

            while len(queue):

                head = queue.popleft()
                for neighbor in cur_edge_map[head]:
                    if neighbor not in visited:

                        visited.add(neighbor)
                        queue.append(neighbor)
                        
            node_visited.extend(visited)          
            res_community.append(visited)
            
    return res_community


def get_modularity(edge_map, res_community, edge_m):
    
    Q_modularity = float(0)
    
    for single_community in res_community:
        for user_i in single_community:
            for user_j in single_community:
                
                if user_i in edge_map[user_j]:
                    A_ij = 1
                else:
                    A_ij = 0
                
                k_i, k_j = len(edge_map[user_i]), len(edge_map[user_j])
                Q_modularity += A_ij - (k_i * k_j) / (2 * edge_m)
                
    return Q_modularity / (2 * edge_m)


def betweenness_to_file(betweenness, OUTPUT_PATH):
    with open(OUTPUT_PATH, 'w') as fp:
        for single_list in betweenness:
            string = str(single_list)[1:][:-1]
            fp.write(string + '\n')
    fp.close()
    return 


def community_to_file(community_store, COMMUNITY_OUTPUT_PATH):
    
    res = [sorted(list(x)) for x in community_store]
    res_comm = sorted(res, key=lambda res: [len(res), res])
    
    with open(COMMUNITY_OUTPUT_PATH, 'w') as fp:
        for line in res_comm:
            fp.write(str(line)[1:][:-1] + '\n')
    fp.close()
    
    return


if __name__ == "__main__":
    
    start_time = time.time()
  
    DATA_FILE = "///Users/tieming/inf553/hw4-py/data/ub_sample_data.csv"
    THRESHOLD = 7
    BETWEEN_OUTPUT_PATH = "///Users/tieming/inf553/hw4-py/task2_betweenness.txt"
    COMMUNITY_OUTPUT_PATH = "///Users/tieming/inf553/hw4-py/task2_community.txt"

    # THRESHOLD = int(sys.argv[1])
    # DATA_FILE = sys.argv[2]
    # BETWEEN_OUTPUT_PATH = sys.argv[3]
    # COMMUNITY_OUTPUT_PATH = sys.argv[4]

    conf = SparkConf().setMaster("local[*]")\
            .setAppName("task2")\
            .set("spark.executor.memory", "4g")\
            .set("spark.driver.memory", "4g")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")


    data_list = readfile(DATA_FILE)
    raw_data_rdd = sc.parallelize(data_list)\
                    .groupByKey()\
                    .mapValues(lambda x: set(x))\
                    .filter(lambda x: len(x[1]) >= 7)


    id_dict = raw_data_rdd.collectAsMap()

    user_rdd = raw_data_rdd.map(lambda line: line[0]).distinct().coalesce(2)

    combined_rdd = user_rdd.cartesian(user_rdd)\
                            .filter(lambda line: line[0] < line[1])

    graph_rdd = combined_rdd.map(lambda line: (line[0], line[1], find_relation(line[0], line[1], id_dict, 7)) )\
                .filter(lambda line: line[2] == "follow")

    user_distinct = graph_rdd.map(lambda line: (line[0], line[1]))\
                .flatMap(lambda x: x)\
                .distinct()\
                .map(lambda x: (x,))

    graph_with_reverse = graph_rdd.map(lambda line: ((line[0], line[1]), (line[1], line[0])) )\
                .flatMap(lambda x: x)


    
    edge_map = graph_with_reverse.groupByKey()\
                            .map(lambda line: (line[0], set(line[1])))\
                            .collectAsMap()

    # calculate betweenness

    user_list = user_distinct.map(lambda x: x[0]).collect()


    betweenness = user_distinct.map(lambda line: level_order(line[0], edge_map))\
            .map(lambda x: create_edge_credit(x[0], edge_map, user_list, x[1]))\
            .flatMap(lambda x:x)\
            .reduceByKey(add)\
            .map(lambda line: [line[0], line[1] / 2])\
            .sortBy(lambda line: [-line[1], line[0][0], line[0][1]])\
            .collect()

    betweenness_to_file(betweenness, BETWEEN_OUTPUT_PATH)

    # community detection
    max_Q = float(-sys.maxsize)
    cur_edge_map = copy.deepcopy(edge_map)
    edge_m = graph_with_reverse.count() / 2

    # adjust edge_map by delete the max betweenness until the map is empty
    modified_m = edge_m

    while modified_m > 0:
        
        betweenness = user_distinct.map(lambda line: level_order(line[0], cur_edge_map))\
                    .map(lambda x: create_edge_credit(x[0], cur_edge_map, user_list, x[1]))\
                    .flatMap(lambda x:x)\
                    .reduceByKey(add)\
                    .map(lambda line: [line[0], line[1] / 2])\
                    .max(key=lambda line: line[1])
        top_betweenness_pair = betweenness[0]
        delete_max_edges(cur_edge_map, top_betweenness_pair)
        
            
        modified_m -= 1
        
        res_community = community_generator(user_list, cur_edge_map)
        local_Q = get_modularity(edge_map, res_community, edge_m)

        if local_Q > max_Q:
            
            max_Q = local_Q
            community_store = copy.deepcopy(res_community)


    community_to_file(community_store, COMMUNITY_OUTPUT_PATH)

    print("Duration: ", time.time() - start_time)