package javademo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaConsumer_ {

    //    private final static String broker = "mini2:9092";
    private final static String broker = "10.112.1.7:6667,10.112.1.21:6667,10.112.1.18:6667,10.112.1.27:6667,10.112.1.33:6667";
    private final static String topic = "test2";
    private final static String groupId = "group2222";

    private final static Logger logger = LoggerFactory.getLogger(KafkaConsumer_.class);

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(new TopicPartition(topic, 0));
        partitions.add(new TopicPartition(topic, 1));
        partitions.add(new TopicPartition(topic, 2));
        consumer.assign(partitions);

        consumer.seek(new TopicPartition(topic, 0), 0);
        consumer.seek(new TopicPartition(topic, 1), 0);
        consumer.seek(new TopicPartition(topic, 2), 0);

//        HashMap<TopicPartition, Long> maps = new HashMap<>();
//        maps.put(new TopicPartition(topic, 0), 1601136000000l);
//        maps.put(new TopicPartition(topic, 1), 1601136000000l);
//        maps.put(new TopicPartition(topic, 2), 1601136000000l);
//
//        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(maps);
//        System.out.println(result);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            while (iter.hasNext()) {

                ConsumerRecord<String, String> record = iter.next();
//                System.out.println(record.value());
                System.out.println();
                logger.info("我消费了一条消息,topic是:" + record.topic() + ",partition是:" + record.partition() +
                        ",offset是:" + record.offset() + ",消息体是:" + record.value());
                Thread.sleep(100);

            }
        }


    }
}
