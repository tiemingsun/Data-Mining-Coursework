import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.json4s._
import org.apache.log4j._
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ListBuffer
import scala.collection.mutable._
import scala.collection.Map
import scala.util.Random
import java.io.File
import java.io.PrintWriter

object task3train extends App {

  def set_to_map(user_set: Array[String]): Map[String, Int] = {
    var user_map: Map[String, Int] = Map()
    for (x <- user_set.indices){
      user_map = user_map + (user_set(x) -> x)
    }
    return user_map
  }


  def index_id_convert(user_index: Map[String, Int]): Map[Int, String] ={
    val index_user: Map[Int, String] = user_index.map{
      case (k,v) => (v, k)
    }
    index_user
  }


  def convert_to_map(line:List[(Int, Float)]): Map[Int,Float] = {
    var output_0: Map[Int, MutableList[Float]] = Map()
    for (single <- line){
      output_0 += (single._1 -> MutableList(0,0))
    }
    for (single <- line){
      output_0(single._1)(0) += single._2
      output_0(single._1)(1) += 1
    }
    val output: Map[Int, Float] = output_0.map{
      case (a,MutableList(b,c)) => (a, b/c)
    }
    output
  }


  def pearson_sim(map1: Map[Int, Float], map2: Map[Int, Float]): Double = {
    val set1 = map1.keys.toSet
    val set2 = map2.keys.toSet
    val common_set = set1.intersect(set2)
    if (common_set.size < 3){
      return -1
    }
    var r1_mean: Float = 0
    var r2_mean: Float = 0
    for (key <- common_set){
      r1_mean += map1(key)
      r2_mean += map2(key)
    }
    r1_mean /= common_set.size
    r2_mean /= common_set.size

    var nominator:Double = 0
    var denominator:Double = 0
    var var_1:Double = 0
    var var_2:Double = 0
    for (key <- common_set){
      nominator += (map1(key) - r1_mean) * (map2(key) - r2_mean)
      var_1 += math.pow(map1(key) - r1_mean, 2)
      var_2 += math.pow(map2(key) - r2_mean, 2)
    }
    denominator = math.sqrt(var_1) * math.sqrt(var_2)
    if (denominator == 0){
      return 0
    }
    val ans = nominator / denominator
    ans
  }


  def generate_hash(num_of_business:Long, numOfHash:Int): List[(Int, Int)] = {
    var output_list = new ListBuffer[(Int, Int)]()
    var r = new Random
    r.setSeed(100)

    for(x <- 1 to numOfHash ){
      val curr_tuple: (Int, Int) = (r.nextInt(num_of_business.toInt), r.nextInt(num_of_business.toInt))
      output_list += curr_tuple
    }
    return output_list.toList
  }

  def minHash(hash_list: List[(Int, Int)], rdd_input: List[Int], num_of_business: Long): List[Int] ={
    var res_list = ListBuffer[Int]()
    for (x <- hash_list){
      var global_min: Int = 999999999
      for (i <- rdd_input){
        var local_min: Int = (x._1 * i + x._2) % num_of_business.toInt
        if (local_min < global_min){
          global_min = local_min
        }
      }
      res_list += global_min

    }
    return res_list.toList
  }


  def lsh(minhash_line:(String, List[Int]), num_of_band: Int): List[((Int, Int), String)] ={
    val bid = minhash_line._1
    val minhash_length = minhash_line._2.size
    val num_of_row = minhash_length / num_of_band
    var res = new ListBuffer[((Int, Int), String)]()

    for (i <- 0 until num_of_band){
      var band_slice = minhash_line._2.slice(i * num_of_row, (i+1) * num_of_row)
      var band_hash = band_slice.sum
      res.+=(((i, band_hash), bid))
    }
    return res.toList
  }


  def separate_candidate(bids: List[String]): List[(String, String)] ={
    val combination = (for {i <- bids.indices
                            j <- 0 until i} yield (bids(i), bids(j)))
    return combination.toList
  }


  def convert_to_file(res_map:Array[Map[String,Any]], path:String) = {

    new PrintWriter(new File(path)) {
      res_map.foreach {
        line =>
          write("{" + "\"b1\": " + line("b1") + ", \"b2\": " + line("b2") + ", \"sim\": " + line("sim") + "}")
          write("\n")
      }
      close()
    }
  }

  def similarity_cand(pair:(String,String), user_bid_map:Map[String,List[Int]]): Double = {
    val set_l = user_bid_map(pair._1).toSet
    val set_r = user_bid_map(pair._2).toSet
    val inter = set_l.intersect(set_r)
    if (inter.size < 3){
      return -1
    }
    inter.size.toFloat / (set_l.size + set_r.size - inter.size).toFloat
  }


  /*
   * Scala Main Function
   */

  val t1 = System.nanoTime()
  Logger.getLogger("org").setLevel(Level.ERROR)
  val train_path: String = "///Users/tieming/inf553/hw3-py/data/train_review.json"
  val model_path: String =  "///Users/tieming/inf553/hw3-sc/task3item.model"
  val cf_type: String = "item_based"
  val sc = new SparkContext("local[*]","task3train")

  val rdd_raw: RDD[Map[String, String]] = sc.textFile(train_path).map(line => parse(line).asInstanceOf[Map[String, String]])
  rdd_raw.take(10).foreach(println)
  // generate business and user map ( bid or uid -> index )
  val business_list: Array[String] = rdd_raw.map(line => line("business_id")).distinct().collect()
  val user_list: Array[String] = rdd_raw.map(line => line("user_id")).distinct().collect()

  val user_index: Map[String, Int] = set_to_map(user_list: Array[String])
  val business_index: Map[String, Int] = set_to_map(business_list: Array[String])

  val index_user: Map[Int, String] = index_id_convert(user_index: Map[String, Int])
  val index_business: Map[Int, String] = index_id_convert(business_index: Map[String, Int])

  if (cf_type == "item_based"){
    val item_cf_rdd: RDD[(Int,Map[Int,Float])] = rdd_raw
      .map(line =>(business_index(line("business_id")), (user_index(line("user_id")), line("stars").toFloat)) )
      .groupByKey()
      .mapValues(line => line.toList)
      .filter(x => x._2.size >= 3)
      .mapValues(x => convert_to_map(x))

    val item_cf_map: Map[Int, Map[Int, Float]] = item_cf_rdd.collectAsMap()

    val business_rdd: RDD[Int] = item_cf_rdd.map(line => line._1).coalesce(2)
    val combined_rdd: RDD[(Int, Int)] = business_rdd.cartesian(business_rdd)
      .filter(pair => pair._1 < pair._2)

    val res_map:Array[Map[String,Any]] = combined_rdd.map(line =>(line, pearson_sim(item_cf_map(line._1), item_cf_map(line._2))))
      .filter(x => x._2 > 0)
      .map(line => Map("b1" ->index_business(line._1._1), "b2"->index_business(line._1._2), "sim"->line._2))
      .collect()

    convert_to_file(res_map:Array[Map[String,Any]], model_path:String)
  }

  // user_based model generator
  else{
    val num_of_business: Long = business_list.length
    val num_of_hash: Int = 30
    val num_of_band: Int = 30
    val similarity: Float = 0.01f
    val hash_list: List[(Int, Int)] = generate_hash(num_of_business:Long, num_of_hash:Int)

    val user_bid_rdd:RDD[(String,List[Int])] = rdd_raw
      .map(line => (line("user_id"), line("business_id")) )
      .groupByKey()
      .mapValues(ids => ids.toList)
      .mapValues(ids => ids.map(string => business_index(string)))



    val user_bid_map = user_bid_rdd.collectAsMap()
    val min_hash_rdd = user_bid_rdd
      .mapValues(ids_index => minHash(hash_list, ids_index, num_of_business))

    val lsh_rdd = min_hash_rdd.map(line => lsh(line, num_of_band))
      .flatMap(x=>x)
      .groupByKey()
      .filter(line =>line._2.size > 1)
      .mapValues(line => line.toList)
      .flatMapValues(separate_candidate)
      .map(line => line._2)
      .distinct()

    val filter_by_jaccard: RDD[(Int,Int)] = lsh_rdd.map(line => (line, similarity_cand(line, user_bid_map)))
      .filter(line => line._2 >= similarity)
      .map(x => x._1)
      .map(x => (user_index(x._1), user_index(x._2)))

    val user_cf_rdd: RDD[(Int,Map[Int,Float])] = rdd_raw
      .map(line =>(business_index(line("user_id")), (user_index(line("business_id")), line("stars").trim.toFloat)) )
      .groupByKey()
      .mapValues(line => line.toList)
      .filter(x => x._2.size >= 3)
      .mapValues(x => convert_to_map(x))

    val user_cf_map: Map[Int, Map[Int, Float]] = user_cf_rdd.collectAsMap()

    val res_map:Array[Map[String,Any]] = filter_by_jaccard.map(line =>(line, pearson_sim(user_cf_map(line._1), user_cf_map(line._2))))
      .filter(x => x._2 > 0)
      .map(line => Map("b1" ->index_user(line._1._1), "b2"->index_user(line._1._2), "sim"->line._2))
      .collect()
    convert_to_file(res_map:Array[Map[String,Any]], model_path:String)

  }
  println("Duration: " + ((System.nanoTime() - t1) / 1e9).toString)
}
