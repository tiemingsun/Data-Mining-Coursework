import org.apache.spark._
import org.apache.spark.SparkContext._
import org.json4s._
import org.apache.log4j._
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.io.File

object task1 {

  // delete punctuation
  def process(line: String, punc_set: Set[String]): String = {
    val trim_line: String = line.trim()
    var res: String = ""

    for(ch <- trim_line){
      if (punc_set.contains(ch.toString) == false){
        res += ch
      }
    }
    return res
  }
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val review_path: String = "///Users/tieming/inf553/hw1-py/review.json"
    val sc = new SparkContext(master="local[*]", appName="task1")

    val input_review = sc.textFile(review_path).map(string => parse(string).values.asInstanceOf[Map[String, String]])
    val input_review_distinct = input_review.map(map => map("review_id")).distinct().count()
    // task 1.1

    println(input_review_distinct)
    // task 1.2
    val input_review_year = input_review.filter(mapline => mapline("date").substring(0,4) == "2017").count()
    println(input_review_year)
    // task 1.3
    val distinct_user_count = input_review.filter(mapline => mapline("text") != None).map(mapline => mapline("user_id"))
      .distinct().count()
    println(distinct_user_count)
    // task 1.4
    val top_users = input_review.map(mapline => (mapline("user_id"), 1)).reduceByKey((x, y) => x+y).sortBy(pair => (-pair._2, pair._1))
      .collect()
    var top_user_name_buffer = new ListBuffer[String]()
    for (i <- 1 to 10){
      top_user_name_buffer += top_users(i)._1
    }
    println(top_user_name_buffer.toList)
    // task 1.5
    val punc_set = Set("(", "[", ",", ".", "!", "?", ":", ";")
    val stopwords_path: String = "///Users/tieming/inf553/hw1-py/stopwords"
    val stopwords_set: Set[String] = Source.fromFile(stopwords_path).getLines().toSet
    val text_words = input_review.map(mapline => process(mapline("text"), punc_set)).filter(word => !stopwords_set.contains(word))
      .map(word => (word, 1)).reduceByKey((x,y)=> x+y).map(pair => pair._1).take(10)
    println(text_words)
  }
}