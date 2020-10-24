package structstreaming_

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object demo3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo3").setMaster("local[3]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


  }
}
