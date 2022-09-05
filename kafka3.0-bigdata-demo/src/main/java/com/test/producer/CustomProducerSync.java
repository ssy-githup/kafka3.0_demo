package com.test.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *  同步发送消息
 */
public class CustomProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 0. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop11:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



        // 1 创建客户端对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //2 发送数据
        //  调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            //抛出异常
            RecordMetadata firstDemo = kafkaProducer.send(new ProducerRecord<String, String>("firstDemo", "test:" + i)).get();
            System.out.println(firstDemo.topic());
        }

        //3 关闭资源
        kafkaProducer.close();
    }
}
