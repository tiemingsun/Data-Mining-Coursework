import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.json4s._
import org.apache.log4j._
import org.json4s.jackson.JsonMethods.parse
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.Map

import java.io.File
import java.io.PrintWriter


object task1 extends App {

  def set_to_map(user_set: Array[String]): Map[String, Int] = {
    var user_map: Map[String, Int] = Map()
    for (x <- user_set.indices){
      user_map = user_map + (user_set(x) -> x)
    }
    return user_map
  }


  def generate_hash(numOfUser:Long, numOfHash:Int): List[(Int, Int)] = {
    var output_list = new ListBuffer[(Int, Int)]()
    var r = new Random
    r.setSeed(100)

    for(x <- 1 to numOfHash ){
      val curr_tuple: (Int, Int) =  (r.nextInt(numOfUser.toInt), r.nextInt(numOfUser.toInt))
      output_list += curr_tuple
    }
    return output_list.toList
  }


  def minHash(hash_list: List[(Int, Int)], rdd_input: List[Int], num_of_user: Long): List[Int] ={
    var res_list = ListBuffer[Int]()
      for (x <- hash_list){
        var global_min: Int = 999999999
        for (i <- rdd_input){
          var local_min: Int = (x._1 * i + x._2) % num_of_user.toInt
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


  def similarity_cand(pair: (String,String), business_user_map: Map[String, List[Int]]): Float={
    val set_l = business_user_map(pair._1).toSet
    val set_r = business_user_map(pair._2).toSet
    val inter = set_l.intersect(set_r)

    return inter.size.toFloat / (set_l.size + set_r.size - inter.size).toFloat
  }

  val t1 = System.nanoTime()
  Logger.getLogger("org").setLevel(Level.ERROR)
  val review_path: String = "///Users/tieming/inf553/hw3-py/data/train_review.json"
  val output_path: String =  "///Users/tieming/inf553/hw3-sc/task1.res"
  val sc = new SparkContext("local[*]","task1")
  val num_of_hash: Int = 35
  val num_of_band: Int = 35
  val similarity: Float = 0.05f


  val rdd_raw: RDD[Map[String, String]] = sc.textFile(review_path)
    .map(string => parse(string).values.asInstanceOf[Map[String, String]])
  //rdd_raw.take(10).foreach(println)
  val num_of_user: Long = rdd_raw.map(line => line("user_id")).distinct().count()
  // create a map: user_id -> index
  val user_set: Array[String] = rdd_raw.map(line => line("user_id")).distinct().collect()
  val user_map: Map[String, Int] = set_to_map(user_set: Array[String])
  val hash_list: List[(Int, Int)] = generate_hash(num_of_user:Long, num_of_hash:Int)

  val business_user_rdd = rdd_raw.map(line => (line("business_id"), line("user_id")) )
    .groupByKey()
    .mapValues(ids => ids.toList)
    .mapValues(ids => ids.map(string => user_map(string)))

  val business_user_map =  business_user_rdd.collectAsMap()

  val min_hash_rdd = business_user_rdd
    .mapValues(ids_index => minHash(hash_list, ids_index, num_of_user))

  val lsh_rdd = min_hash_rdd.map(line => lsh(line, num_of_band))
    .flatMap(x=>x)
    .groupByKey()
    .filter(line =>line._2.size > 1)
    .mapValues(line => line.toList)
    .flatMapValues(separate_candidate)
    .map(line => line._2)
    .distinct()

  val similar_map = lsh_rdd.map(pair => (pair, similarity_cand(pair, business_user_map)))
    .filter(line => line._2 >= similarity)
    .map(line => Map("b1" ->line._1._1, "b2"->line._1._2, "sim"->line._2))
    .collect()

  println("Accuracy for LSH is: " + similar_map.size.toFloat / 59435.toFloat)
  println("Duration: " + ((System.nanoTime() - t1) / 1e9).toString)
  new PrintWriter(new File(output_path)){
    similar_map.foreach{
       line =>
        write("{" + "\"b1\": " + line("b1") + ", \"b2\": " + line("b2") + ", \"sim\": " + line("sim") + "}")
        write("\n")
    }
    close()
  }
}
