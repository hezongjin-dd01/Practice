package structstreaming_

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

case class socket(value: String, timestamp: Timestamp)

object demo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo").setMaster("local[3]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val lines = spark.readStream
      .format("socket")
      //      .option("host", "10.112.0.8")
      .option("host", "mini1")
      .option("port", "1122")
      .option("includeTimestamp", true)
      .load()

    import spark.implicits._


    val words = lines.as[socket].flatMap(iter => {
      val strs = iter.value.split(" ")
      val array = new ArrayBuffer[socket]

      for (str <- strs) {
        array += socket(str, iter.timestamp)
      }

      array.iterator
    })

    /* val qq = words.writeStream
       .outputMode("update")
       .format("console")
       .start()
       qq.awaitTermination()
    */


    val wordCounts = words.groupBy("value", "timestamp").count()
    wordCounts.printSchema()

    val query = wordCounts.writeStream
      //      .outputMode("update")
      //            .format("console")
      .format("kafka")
      .option("kafka.bootstrap.servers", "mini2:9092")
      .option("topic", "test")
      .start()


    query.awaitTermination()

    val w2 = wordCounts.withWatermark("", "")


    lines.groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word"
    ).count()


    //it's been a long day
    //without you my friend


    /**
      *
      */

  }
}
