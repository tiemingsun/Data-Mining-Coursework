from pyspark import SparkContext
import json
import sys


def add_count(x, y):
    return x[0] + y[0], x[1] + y[1]


def flat_category(line):
    return [(category, line[1][0]) for category in line[1][1] ]


def top_n_categories_with_spark(input_review, input_business, n):
    input_review = input_review.map(lambda lines: json.loads(lines))
    input_business = input_business.map(lambda lines: json.loads(lines))

    star_businessid = input_review.map(lambda line: (line['business_id'], (line['stars'], 1) ))\
                                  .reduceByKey(add_count)

    businessid_categories = input_business.map(lambda line: (line['business_id'], line['categories']))\
                                          .filter(lambda line: line[0] != None and line[1] != None)\
                                          .mapValues(lambda categories: [category.strip() for category in categories.split(',')] )

    
    bussi_star_categories = star_businessid.leftOuterJoin(businessid_categories)

    categories_result = bussi_star_categories.filter(lambda line: line[1][0] != None and line[1][1] != None)\
                                             .flatMap(flat_category)\
                                             .reduceByKey(add_count)\
                                             .mapValues(lambda values: values[0]/values[1])

    return categories_result.takeOrdered(n, key=lambda line: [-line[1], line[0] ])


def review_dataset_process(input_review_raw):
    input_review_list = [ {'business_id': line['business_id'], 'stars': line['stars']} for line in input_review_raw ]
    
    return input_review_list


def business_dataset_process(input_business_list):
    input_business_list = [{'business_id': line['business_id'], 'categories': line['categories'] } for line in input_business_raw]
    for line in input_business_list:
        if line['categories']:
            list_line= line['categories'].split(',')
            line['categories'] = [word.strip() for word in list_line]
        else:
            line['categories'] = []

    return input_business_list


def join(input_business_list, categories_dict, business):
    for line in input_business_list:
        b_id = line['business_id']
        categories = line['categories']
        
        cur_star, cur_count = business[b_id]
        for cate in categories:
            categories_dict[cate][0] += cur_star
            categories_dict[cate][1] += cur_count
    return categories_dict


def sort_top_n_category(categories_dict, n):
    for key in categories_dict:
        if categories_dict[key][1]:
            categories_dict[key] = categories_dict[key][0] / categories_dict[key][1]
        else:
            categories_dict[key] = 0
    
    categories_key_value = [ [k,v] for k, v in categories_dict.items() ]
    categories_key_value = sorted(categories_key_value, key= lambda x: [-x[1],x[0]])
    return categories_key_value[:n]


if __name__ == "__main__":

    # system arguments inputs
    # spark-submit task2.py ///Users/tieming/inf553/hw1-pyspark/review.json ///Users/tieming/inf553/hw1-pyspark/business.json ///Users/tieming/inf553/hw1-pyspark/output_2.json no_spark 6
    review_json_path = sys.argv[1]
    business_json_path = sys.argv[2]
    output_file_path = sys.argv[3]
    if_spark = sys.argv[4]
    n = sys.argv[5]

    sc = SparkContext(master="local", appName="First_app")

    # review_json_path = "///Users/tieming/inf553/hw1-pyspark/review.json"
    # business_json_path = "///Users/tieming/inf553/hw1-pyspark/business.json"
    # output_file_path = "///Users/tieming/inf553/hw1-pyspark/output_2.json"
    # if_spark = "no_spark"
    # n = 6

    # compute the average stars for each business category and 
    # output top n categories with the highest average stars -- with spark
    if if_spark == "spark":

        input_review = sc.textFile(review_json_path)
        input_business = sc.textFile(business_json_path)

        result_list = top_n_categories_with_spark(input_review, input_business, int(n))
    

    # compute the average stars for each business category and 
    # output top n categories with the highest average stars -- without spark
    elif if_spark == "no_spark":

        input_review_raw = [json.loads(line) for line in open(review_json_path, 'r')]
        input_business_raw = [json.loads(line) for line in open(business_json_path, 'r')]

        input_review_list = review_dataset_process(input_review_raw)
        input_business_list = business_dataset_process(input_business_raw)

        # generate dictionary for business, example: {'-I5umRTkhw15RqpKMl_o1Q': [0, 0]}, 
        # first entry => the total_star, second entry => the total_num for this business_id
        business = {}
        for line in input_business_list:
            business[line['business_id']] = [0,0]
        for line in input_review_list:   
            if line['business_id'] not in business:
                business[line['business_id']] = [0,0] # [total_star, total_num]
            business[line['business_id']][0] += line['stars']
            business[line['business_id']][1] += 1

        # generate dictionary for categories. example: {'Astrologers': [0, 0]}, similar with business 
        # dictionary first entry => the total_star, second entry => the total_num for this category
        categories_dict = {}
        for line in input_business_list:
            for category in line['categories']: 
                if category not in categories_dict:
                    categories_dict[category] = [0,0]

        
        categories_dict = join(input_business_list, categories_dict, business)
        result_list = sort_top_n_category(categories_dict, int(n))

    else:
        result_list = []
    
    result_dict = {}
    result_dict['result'] = result_list

    with open(output_file_path, 'w+') as output_file:
        json.dump(result_dict, output_file)
    output_file.close()
