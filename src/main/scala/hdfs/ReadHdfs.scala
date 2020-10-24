package hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source


object ReadHdfs {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    //设置 这个参数 或者添加core-site.xml到resource文件夹里面
    conf.set("fs.defaultFS", "hdfs://mini1:9000")

    val path = new Path("hdfs://mini1:9000/data/entId.txt")
    val fs = FileSystem.get(conf)

    val stream = fs.open(path)
    val strings = Source.fromInputStream(stream).getLines()

    while (strings.hasNext) {
      println(strings.next())
      Thread.sleep(1000)
    }

    val confSpark = new SparkConf().setAppName("demo").setMaster("local[3]")
    val spark = SparkSession.builder().config(confSpark).getOrCreate()

    val rdd = spark.sparkContext.textFile("hdfs://mini1:9000/data/entId.txt")

    val strArray = rdd.collect()



  }
}
