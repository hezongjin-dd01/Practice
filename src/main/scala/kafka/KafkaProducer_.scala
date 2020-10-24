package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object KafkaProducer_ {

  //  val brokerServers = "mini2:9092";
  //  val brokerServers = "10.112.0.7:6667,10.112.0.8:6667,10.112.0.9:6667"
  private val brokerServers = "10.112.1.7:6667,10.112.1.21:6667,10.112.1.18:6667,10.112.1.27:6667,10.112.1.33:6667"

  val topic = "ce10PaidOrder"
  //  val topic = "test"

  def main(args: Array[String]): Unit = {

    val properties = new Properties()

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers)

    var producer: KafkaProducer[String, String] = null;
    producer = new KafkaProducer[String, String](properties)


    try {
      while (true) {
        Thread.sleep(10000)
        for (i <- 0 to 1000) {

          val id = Random.nextInt(100018463) + 10000

          val cid = Random.nextInt(323) + 10000

          val value =
            s"""
               |{"channel_info_id":"2","received_detail":"富源里小区","is_consume_balance":"1","create_user_name":"","send_type":"1","consume_balance_money":"552.66","received_city_name":"北京市","delivery_way":"1","order_status":"0","id":"${id}","is_join_group":"0","delete_flag":"1","order_type":"0","consume_score":"0","member_sn":"C3230427","create_user_id":"1158625355666430121","create_time":"2020-10-23 10:51:47","received_province_name":"北京","update_user_name":"","gain_score":"0","buyer_remark":"","consume_score_convert_money":"0.0","receivable_money":"0.0","received_name":"肖肖","cid":"${cid}","code":"D323474452524565266432","active_id":"0","freight":"0.0","order_product_names":"华为pro","received_province_code":"110000","pay_way":"5","is_exceed_after_sale":"0","update_time":"2020-10-09 15:51:47","update_user_id":"1158625355666430121","received_phone":"18920140389","is_invoice":"0","activity_list_json":"[]","actual_money":"0.0","receive_address":"北京北京市大兴区","member_info_id":"1158625355666430121","channel_name":"公众号","discount_money":"0.0","is_right_package_freight":"0","replace_pay_way":"0","received_area_name":"大兴区","is_express_electronic_sheet":"0","received_city_code":"110100","goods_amount":"552.66","member_name":"凉城小街姑娘","received_area_code":"110115","is_consume_score":"0","is_out_stock":"0","consume_coupon_code_money":"0.0"}
                  """.stripMargin.trim

          //        val value =
          //          """
          //            |{"page_url":"pages/classifyPage/goodDetail/goodDetail","member_info_id":1158625355666448411,"time_stamp":1603416577857,"open_id":"oQYe25DabWkYNw9XagWJtCparQK0","product_info_id":1156003937296460092,"channel":1,"union_id":"osFcvuPXI-yIS7o0DxPuCFsc9Z5Q","goods_info_id":1156003937648783640,"cid":9999}
          //          """.stripMargin.trim
          //        val record = new ProducerRecord[String, String](topic, "{\"timestamp\":\"1562209583\",\"level\":\"2\",\"message\":\"hello2\"}")
          val record = new ProducerRecord[String, String](topic, value)

          producer.send(record)
          //        Thread.sleep(10)
        }
        producer.flush()
      }
    }
    catch {
      case ex: Exception =>
        println(ex)
    }
    finally {
      //close也能刷新数据
      if (producer != null) {
        producer.close()
      }
    }

  }
}
