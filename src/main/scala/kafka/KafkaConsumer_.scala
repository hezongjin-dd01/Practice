package kafka

import java.util
import java.util.{Collections, Properties}

import javax.management.remote.JMXServiceURL
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{JavaConversions, mutable}

object KafkaConsumer_ {

  //  val brokerServer = "192.168.91.4:9092"
  val brokerServer = "10.112.0.7:6667,10.112.0.8:6667,10.112.0.9:6667"
  val topic = "test2"
  //  val topic = "__consumer_offsets"
  val groupId = "myConsumer"

  private val logger: Logger = LoggerFactory.getLogger(KafkaConsumer_.getClass)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")


    val consumer = new KafkaConsumer[String, String](properties)

    val partitions = new util.ArrayList[TopicPartition]()
    partitions.add(new TopicPartition(topic, 0))
    partitions.add(new TopicPartition(topic, 1))
    partitions.add(new TopicPartition(topic, 2))
    //    partitions.add(new TopicPartition(topic, 3))
    //    partitions.add(new TopicPartition(topic, 4))
    //    partitions.add(new TopicPartition(topic, 5))
    //    partitions.add(new TopicPartition(topic, 6))
    //    partitions.add(new TopicPartition(topic, 7))
    //    partitions.add(new TopicPartition(topic, 8))
    //    partitions.add(new TopicPartition(topic, 9))
    //    partitions.add(new TopicPartition(topic, 10))
    //    partitions.add(new TopicPartition(topic, 11))
    //    partitions.add(new TopicPartition(topic, 12))
    //    partitions.add(new TopicPartition(topic, 13))
    //    partitions.add(new TopicPartition(topic, 14))
    //    partitions.add(new TopicPartition(topic, 15))
    //    partitions.add(new TopicPartition(topic, 16))
    //    partitions.add(new TopicPartition(topic, 17))
    consumer.assign(partitions)
    //    consumer.seekToBeginning(partitions)


    //    val partitionToLong = consumer.endOffsets(partitions)
    //    println(partitionToLong)


    //    val partitions = new util.ArrayList[TopicPartition]()
    //    partitions.add(new TopicPartition("test", 0))
    //    partitions.add(new TopicPartition("test", 1))
    //    consumer.assign(partitions)
    //    consumer.seekToBeginning(partitions)
    //
    //
    //    consumer.subscribe(Collections.singletonList(topic))
    //
    consumer.seek(new TopicPartition(topic, 0), 0)
    consumer.seek(new TopicPartition(topic, 1), 0)
    consumer.seek(new TopicPartition(topic, 2), 0)
    //
    //
    //    val partitions2 = new util.ArrayList[TopicPartition]()
    //    partitions2.add(new TopicPartition("test", 0))
    //    partitions2.add(new TopicPartition("test", 1))
    //    consumer.seekToEnd(partitions2)
    //
    //    consumer.seekToBeginning(partitions2)
    //
    //    consumer.seek(new TopicPartition("test", 0), 0)
    //    consumer.seek(new TopicPartition("test", 1), 0)
    //    //    consumer.seek(new TopicPartition("test", 1), 0)
    //
    //    consumer.seekToEnd(partitions2)
    //
    //
    //    //    val stringToInfoes = consumer.listTopics()
    //    //    println(stringToInfoes)
    //
    //
    //    //    val infoes = consumer.partitionsFor(topic)
    //    //    println(infoes)
    //
    //

    //    consumer.subscribe(Collections.singletonList(topic))
    //
    while (true) {
      val records = consumer.poll(1000)

      val iter = records.iterator()
      while (iter.hasNext) {

        val record = iter.next()

        Thread.sleep(10)
        println(record)
        //        if (record.offset() == 0) {
        //          logger.info(s"offset为=======>,${record.offset()},partition:${record.partition()},value:${record.value()}")
        //        } else {
        //          logger.info(s"offset为,${record.offset()},partition:${record.partition()},value:${record.value()}")
        //        }

      }
    }

    //    val partitions = new util.ArrayList[TopicPartition]()
    //    partitions.add(new TopicPartition("test", 0))
    //    partitions.add(new TopicPartition("test", 1))
    //    partitions.add(new TopicPartition("test", 2))
    //    val partitionToLong = consumer.endOffsets(partitions)
    //    println(partitionToLong)

    //
    //    val partitionToLong = consumer.beginningOffsets(partitions)
    //    println(partitionToLong)

    //    val topics = consumer.listTopics()
    //
    //    println(topics)


  }
}
