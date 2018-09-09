package com.joey.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by xiaowu.zhou@tongdun.cn on 2018/9/4.
 */
public class KafkaConsumerDemo extends Thread{


    private final KafkaConsumer kafkaConsumer;

    public KafkaConsumerDemo(String topic) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"KafkaConsumerDemo");
        //false需要手工确认
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {

        while (true){

            ConsumerRecords<Integer , String> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord consumerRecord : records){
                System.out.println(" message received:"+consumerRecord.value());

                //"partition:"+ consumerRecord.partition()  +
                //手工确认
//                kafkaConsumer.commitAsync();
            }

        }
    }

    public static void main(String[] args) {
        new KafkaConsumerDemo("test").start();
    }
}
