package kafka

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object Demo4 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("demo4").setMaster("local[4]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    import spark.implicits._
    val df = rdd.toDF("id")

    df.show()

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv("/csv")

  }
}
