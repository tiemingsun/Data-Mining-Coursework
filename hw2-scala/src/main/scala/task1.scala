import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer
import scala.io.Source

object task1 {
  def readFile(TEST_FILE: String): ListBuffer[Array[String]] ={
    val src = Source.fromFile(TEST_FILE)
    val iter = src.getLines().map(_.split(","))
    var isHead = 1
    var out_list = new ListBuffer[Array[String]]()
    while(iter.hasNext){
      if (isHead == 1){
        iter.next()
        isHead = 0
      }
      out_list += iter.next()
    }
    src.close()
    println((out_list))
    return out_list
  }
  def main(args: Array[String]): Unit = {
    //    val startTime = System.currentTimeMillis
    //    val case_num = args(1).toInt
    //    val s = args(2).toInt
    //    val TEST_FILE = args(3)
    //    val OUTPUT_FILE = args(4)
    val TEST_FILE = "///Users/tieming/inf553/hw2-py/asnlib/publicdata/small1.csv"
    readFile(TEST_FILE)
    val conf = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(conf)

  }



}

