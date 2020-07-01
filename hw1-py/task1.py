from pyspark import SparkContext
import os
import json
import sys
from operator import add


def num_of_review_year(input_review, year):
    input_review_date = input_review.map(lambda line: (line["date"], line["review_id"]))
    return input_review_date.filter(lambda s: s[0][:4] == str(year)).count()


def top_m_user_with_largest_review(input_review, m):
    input_user_topm_review = input_review.map(lambda line: (line["user_id"], 1) ).reduceByKey(add)
    return input_user_topm_review.takeOrdered(m, key=lambda x: -x[1])


def delete_stopwords_and_punc(words, punc_set, stop_word_set):
    
    if words in punc_set:
        return ''
    words_without_punc = ''.join(c for c in words if c not in punc_set)
    if words_without_punc not in stop_word_set:
        return words_without_punc
    else:
        return ''


def top_n_words_without_stopwords(input_review, n, punc_set, stop_word_set):

    # punctuations may connected with words or separated with single
    # first clear the punctuations, then delete stop words

    input_with_punc_stop = input_review.map(lambda line: line["text"])\
                                       .flatMap(lambda sentence: sentence.lower().split(' '))

    input_without_punc_stop = input_with_punc_stop.map(lambda words: (delete_stopwords_and_punc(words, punc_set, stop_word_set), 1))

    top_n_words = input_without_punc_stop.filter(lambda word_tuple: word_tuple[0] is not '')\
                                         .reduceByKey(add)\
                                         .takeOrdered(n, key=lambda x: -x[1])
    return [x[0] for x in top_n_words]
    

if __name__ == "__main__":

    sc = SparkContext(master="local", appName="First_app")
    # spark-submit task1.py ///Users/tieming/inf553/hw1-pyspark/review.json ///Users/tieming/inf553/hw1-pyspark/output_1.json ///Users/tieming/inf553/hw1-pyspark/stopwords 2017 3 3
    # system arguments inputs

    review_path = sys.argv[1]
    output_file_path = sys.argv[2]
    stopwords_file = open(sys.argv[3], 'r')
    year = str(sys.argv[4])
    m = int(sys.argv[5])
    n = int(sys.argv[6])

    # paths and file input
    # review_path = "///Users/tieming/inf553/hw1-pyspark/review.json"
    # stopwords = "///Users/tieming/inf553/hw1-pyspark/stopwords"
    # output_file_path = "///Users/tieming/inf553/hw1-pyspark/output_1.json"
    # year = 2017
    # m = 10
    # n = 10
    # stopwords_file = open('stopwords', 'r')

    input_review = sc.textFile(review_path)
    
    punc_set = {'(', '[', ',', '.', '!', '?', ':', ';', ']', ')'}
    stop_word_set = set(word.strip() for word in stopwords_file)
    input_review = input_review.map(lambda lines: json.loads(lines))

    

    

    # A. the total number of reviews
    input_review_id = input_review.map(lambda line: line["review_id"])
    total_review_count = input_review_id.count()

    # B. The number of reviews in a given year, y
    review_in_year = num_of_review_year(input_review, year )

    # C. The number of distinct users who have written the reviews
    input_user_id = input_review.filter(lambda line: line["text"]!=None)\
                                .map(lambda line: line["user_id"])\
                                .distinct()
    distinct_user = input_user_id.count()

    # D. Top m users who have the largest number of reviews and its count
    top_m_user_review = top_m_user_with_largest_review(input_review=input_review, m=m)

    # E. Top n frequent words in the review text. Exclude stop words, in lower
    # case, clear punctuations
    top_n_words = top_n_words_without_stopwords(input_review, n, punc_set, stop_word_set)

    result_dict = {}
    result_dict['A'] = total_review_count
    result_dict['B'] = review_in_year
    result_dict['C'] = distinct_user
    result_dict['D'] = top_m_user_review
    result_dict['E'] = top_n_words

    with open(output_file_path, 'w+') as output_file:
        json.dump(result_dict, output_file)
    output_file.close()