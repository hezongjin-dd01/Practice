package javademo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.xerial.snappy.buffer.CachedBufferAllocator;

import java.util.*;

public class KafkaConsumer2_ {

    //    private final static String broker = "mini2:9092";
//        private final static String broker = "10.20.127.63:6667,10.20.127.64:6667,10.20.127.65:6667";
//    private final static String broker = "10.112.0.7:6667,10.112.0.8:6667,10.112.0.9:6667";
    private final static String broker = "10.112.1.7:6667,10.112.1.21:6667,10.112.1.18:6667,10.112.1.27:6667,10.112.1.33:6667";
    private final static String topic = "ce10UserBehavior";
    //    private final static String topic = "ce10OrderAfterSale";
    //    private final static String topic = "ce10PaidOrder";
    private final static String groupId = "group22223322";

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


//        getOffset(consumer);


        getValue(consumer);

    }

    private static void getValue(KafkaConsumer<String, String> consumer) {
        consumer.seek(new TopicPartition(topic, 0), 10600);
        consumer.seek(new TopicPartition(topic, 1), 10600);
        consumer.seek(new TopicPartition(topic, 2), 10600);

        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            Iterator<ConsumerRecord<String, String>> iter = records.iterator();
            while (iter.hasNext()) {

                ConsumerRecord<String, String> record = iter.next();
//                if (record.value().contains("退单测试")) {
//                    count += 1;
//                    System.out.println(record.value());
//                    System.out.println(count);
//                }

                System.out.println(record.value());
            }
        }
    }

    private static void getOffset(KafkaConsumer<String, String> consumer) {

        Calendar instance = Calendar.getInstance();
        instance.set(Calendar.HOUR_OF_DAY, 0);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        long timestamp = instance.getTimeInMillis();
        HashMap<TopicPartition, Long> maps = new HashMap<>();
        maps.put(new TopicPartition(topic, 0), timestamp);
        maps.put(new TopicPartition(topic, 1), timestamp);
        maps.put(new TopicPartition(topic, 2), timestamp);

        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(maps);
        System.out.println(result);
    }
}
