import java.io.{File, FileWriter}
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object task1 {

  def readFile(TEST_FILE: String): ArrayBuffer[Array[String]] ={
    val src = Source.fromFile(TEST_FILE)
    val iter = src.getLines().map(_.split(","))
    var isHead = 1
    var out_list = new ArrayBuffer[Array[String]]()
    while(iter.hasNext){
      if (isHead == 1){
        iter.next()
        isHead = 0
      }
      out_list +=  iter.next()
    }
    src.close()
    println((out_list))
    return out_list
  }
  def generate_basket(case_num: Int, x: Array[String]): Array[String] ={
    if(case_num == 2){
      val temp = x(1)
      x(1) = x(0)
      x(0) = temp
    }
    return x
  }

  def find_freq(chunk: Iterator[Set[String]], value1: Any, i: Int, i1: Int, array: Array[Nothing])

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis
    //    val case_num = args(1).toInt
    //    val s = args(2).toInt
    //    val TEST_FILE = args(3)
    //    val OUTPUT_FILE = args(4)

    val case_num = 1
    val s = 4
    val num_partition = 3
    val bitmap_num = 2

    val TEST_FILE = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/small1.csv"
    var raw = readFile(TEST_FILE)
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(raw).cache()

    val basket_rdd = rdd.map(x => generate_basket(case_num, x))
                        .groupByKey()
                        .mapValues(x => x.toSet())
                        .mapValues(x => x.toArray()).mapValues(x => x.sorted(_ < _)).persist()

    val numOfBasket = basket_rdd.collect().length
    val basket_list = basket_rdd.collect()

    val map1 = basket_rdd.map(x => (map_func(x), x)).partitionBy(num_partition)
      .mapPartitions(chunk => find_freq(chunk.toArray(), numOfBasket, s, bitmap_num, Array()))
      .flatMap(x => x)

    val reduce1 = map1.distinct().persist()



//    basket_rdd = rdd_1.map(lambda line: generate_basket(case_num, line)).groupByKey().mapValues(set).mapValues(list).map(lambda x: x[1])\
//    .map(lambda x: sorted(x)).persist()
//    map1 = basket_rdd.map(lambda line: (map_func(line), line)).partitionBy(num_partition)\
//    .mapPartitions(lambda chunk: find_freq_pcy(list(chunk), numOfBasket, s, bitmap_num, []) )\
//      .flatMap(lambda x: x)
  }



}

object jiaqi_fan_task1 {

  def combinerPairs(input: List[String]): Set[List[String]] = {
    var newPairs = Set[List[String]]()
    for(i <- 0 until input.length ){
      for (j <- i+1 until input.length){
        newPairs += List(input(i),input(j)).sorted
      }
    }
    return newPairs
  }

  def combineTriple(input: List[List[String]], size: Int): Set[List[String]] ={
    var newTriple = Set[List[String]]()
    for (i <- 0 until(input.length)){
      for(j <- i+1 until(input.length)) {
        var aset = Set[String]()

        for (ele <- input(i))
          aset += ele
        for (ele <- input(j))
          aset += ele

        if(aset.size == size)
          newTriple += aset.toList.sorted
      }
    }

    return newTriple
  }

  def aPriori(iterator: Iterator[Set[String]], ps: Float): Iterator[(List[String], Int)] ={
    var candidates = scala.collection.mutable.ListBuffer.empty[(List[String], Int)]
    var baskets = scala.collection.mutable.ListBuffer.empty[Set[String]]
    var num_basket = 0
    var single_set = Set[String]()

    while(iterator.hasNext) {
      var basket = iterator.next()
      baskets += basket
      num_basket += 1
      for (ele <- basket) {
        single_set += ele
      }
    }
    val threshold = ps * num_basket

    var fre_single = Set[String]()
    for (i <- single_set){
      var count = 0
      for(j <- baskets){
        if(Set(i).intersect(j) == Set(i)){
          count += 1
        }
      }
      if (count >= threshold){
        fre_single += i
        candidates += Tuple2(List(i), 1)
      }
    }

    var size = 2
    var pairSet = combinerPairs(fre_single.toList)
    var fre_pair = Set[List[String]]()
    for (i <- pairSet){
      var count = 0
      for (j <- baskets){
        if ( j.intersect(i.toSet) == i.toSet){
          count += 1
        }
      }
      if(count >= threshold){
        fre_pair += i
        candidates += Tuple2(i, 1)
      }
    }

    while(!fre_pair.isEmpty){
      size += 1
      var triple_set = combineTriple(fre_pair.toList, size)
      fre_pair = Set[List[String]]()
      for (i <- triple_set){
        var count = 0
        for (j <- baskets){
          if ( j.intersect(i.toSet) ==  i.toSet){
            count += 1
          }
        }
        if(count >= threshold){
          fre_pair += i
          candidates += Tuple2(i, 1)
        }
      }
    }
    return candidates.iterator
  }

  def countFre(iterator: Iterator[List[String]] , baskets: Array[Set[String]]):  Iterator[(List[String], Int)] = {
    var fre = scala.collection.mutable.ListBuffer.empty[(List[String], Int)]
    while(iterator.hasNext){
      val pair = iterator.next()
      var count = 0
      for(i <- baskets){
        if(i.intersect(pair.toSet) == pair.toSet){
          count += 1
        }
      }
      fre += Tuple2(pair, count)
    }
    return fre.iterator
  }


  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()

    val conf = new SparkConf()
      .setAppName("INF553_hw2")
      .setMaster("local[*]")
    val sc =  new SparkContext(conf)
    //    val case_number = args(0)
    //    val support = args(1)
    //    val data = sc.textFile(args(2))
    //    val output = new PrintWriter(args(3))
    val case_number = 2
    val support = 7
    val data = sc.textFile("/Users/frank/Desktop/553/data/test_data.csv").cache()
    val output = new PrintWriter("output1.txt")

    val header = data.first()
    var preData = data.filter(record => record != header).map(record => record.split(","))

    var intermediate_result_output = scala.collection.mutable.ListBuffer.empty[List[String]]
    var final_result_output = scala.collection.mutable.ListBuffer.empty[List[String]]


    if (case_number == 1){
      val newData = preData.map(x=>(x(0),x(1))).groupByKey().mapValues(x=> x.toSet).values.cache()

      val baskets = newData.collect()
      val num_baskets = newData.count()
      var ps = support.toFloat / num_baskets.toFloat

      val pass1 = newData.mapPartitions(chunk => aPriori(chunk, ps)).reduceByKey((x,y) => x +y).keys
      val pass2 = pass1.mapPartitions(fre => countFre(fre,baskets)).filter(x=> x._2 >= support).keys

      val intermediate_result = pass1.map(x => (x.length, x)).groupByKey().mapValues(x=> x.toList).sortByKey().collect()
      val final_result = pass2.map(x => (x.length, x)).groupByKey().mapValues(x=> x.toList).sortByKey().collect()

      for(i <- intermediate_result){
        val value = i._2
        var set = Set[String]()
        for (list_pair <- value){
          var string = ""
          string += "("
          val last_one = list_pair.last
          for (ele <- list_pair){
            if (ele == last_one){
              string += "\'"+ele+"\'"
            }else{
              string += "\'"+ele+"\', "
            }
          }
          string += ")"
          set += string
        }
        intermediate_result_output += set.toList.sorted
      }

      for(i <- final_result){
        val value = i._2
        var set = Set[String]()
        for (list_pair <- value){
          var string = ""
          string += "("
          val last_one = list_pair.last
          for (ele <- list_pair){
            if (ele == last_one){
              string += "\'"+ele+"\'"
            }else{
              string += "\'"+ele+"\', "
            }
          }
          string += ")"
          set += string
        }
        final_result_output += set.toList.sorted
      }
    }else if(case_number == 2){
      val newData = preData.map(x=>(x(1),x(0))).groupByKey().mapValues(x=> x.toSet).values
      val baskets = newData.collect()
      val num_baskets = newData.count()
      var ps = support.toFloat / num_baskets.toFloat

      val pass1 = newData.mapPartitions(chunk => aPriori(chunk, ps)).reduceByKey((x,y) => x +y).keys
      val pass2 = pass1.mapPartitions(fre => countFre(fre,baskets)).filter(x=> x._2 >= support).keys

      val intermediate_result = pass1.map(x => (x.length, x)).groupByKey().mapValues(x=> x.toList).sortByKey().collect()
      val final_result = pass2.map(x => (x.length, x)).groupByKey().mapValues(x=> x.toList).sortByKey().collect()

      for(i <- intermediate_result){
        val value = i._2
        var set = Set[String]()
        for (list_pair <- value){
          var string = ""
          string += "("
          val last_one = list_pair.last
          for (ele <- list_pair){
            if (ele == last_one){
              string += "\'"+ele+"\'"
            }else{
              string += "\'"+ele+"\', "
            }
          }
          string += ")"
          set += string
        }
        intermediate_result_output += set.toList.sorted
      }

      for(i <- final_result){
        val value = i._2
        var set = Set[String]()
        for (list_pair <- value){
          var string = ""
          string += "("
          val last_one = list_pair.last
          for (ele <- list_pair){
            if (ele == last_one){
              string += "\'"+ele+"\'"
            }else{
              string += "\'"+ele+"\', "
            }
          }
          string += ")"
          set += string
        }
        final_result_output += set.toList.sorted
      }
    }else{
      println("Wrong command")
      System.exit(1)
    }

    output.write("Intermediate Itemsets:\n")
    for(list <- final_result_output){
      val last_one = list.last
      for (ele <- list){
        if(last_one == ele){
          output.write(ele + "\n")
        }else{
          output.write(ele + ", ")
        }
      }
      output.write("\n")
    }

    output.write("Frequent Itemsets:\n")
    for(list <- final_result_output){
      val last_one = list.last
      for (ele <- list){
        if(last_one == ele){
          output.write(ele + "\n")
        }else{
          output.write(ele + ", ")
        }
      }
      output.write("\n")
    }
    output.close()

    val end_time = System.currentTimeMillis()
    println("Duration: " + (end_time-start_time)/1000)


  }
}

