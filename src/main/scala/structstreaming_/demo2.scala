package structstreaming_

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

object demo2 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("demo2").setMaster("local[3]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val line = spark.readStream
      .format("socket")
      .option("host", "mini1")
      .option("port", "1122")
      .option("includeTimestamp", true)
      .load()

    import spark.implicits._
    val lines = line.as[(String, Timestamp)]

    //    val query = lines.writeStream
    //      .outputMode("append")
    //      .format("json")
    //      .option("checkpointLocation", "path/to/checkpoint/dir")
    //      .option("path", "path/to/destination/dir")
    //      .start()
    val query = line.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "mini2:9092")
      .option("topic", "test")
      .start()

    query.awaitTermination()


    lines.writeStream
      .foreach(
        new ForeachWriter[(String, Timestamp)] {
          override def open(partitionId: Long, version: Long): Boolean = {
            //获得连接
            try {
              true
            } catch {
              case _ => false
            }
          }

          override def process(value: (String, Timestamp)): Unit = {
            //写入数据
          }

          override def close(errorOrNull: Throwable): Unit = {
            //关闭连接
          }
        }

      )
  }
}
