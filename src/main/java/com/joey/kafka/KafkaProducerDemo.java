package com.joey.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by xiaowu.zhou@tongdun.cn on 2018/9/4.
 */
public class KafkaProducerDemo extends Thread{

    private final String topic;

    private final boolean isAsync;

    private final KafkaProducer<Integer,String> kafkaProducer;

    public KafkaProducerDemo(String topic, boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerDemo");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<Integer, String>(properties);
    }

    @Override
    public void run() {

        int num = 0;

        while (num<50){

            String message ="message_"+num;
            System.out.println("begin send message:"+message);

            if (isAsync){//yibufasong

                kafkaProducer.send(new ProducerRecord<Integer, String>(topic, num , message), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {

                        if (metadata !=null){
                            System.out.println("async-offset:"+metadata.offset() + "->partition:"+metadata.partition());
                        }
                    }
                });

            }else {

                try {
                    RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<Integer, String>(topic,message)).get();
                    System.out.println("sync-offset:"+metadata.offset() + "->partition:"+metadata.partition());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            }

            num++;

        }

    }


    public static void main(String[] args) {
        new KafkaProducerDemo("test",true).start();
    }

}
