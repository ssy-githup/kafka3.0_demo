package com.test.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBack {
    public static void main(String[] args) {

        // 0. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop11:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



        // 1 创建客户端对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //2 发送数据
        //  调用 send 方法,发送消息
        for (int i = 0; i < 500; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("firstDemo", "test:" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //如果 Exception 为 null 说明消息发送成功，如果 Exception 不为 null，说明消息发送失败
                    if (e == null){
                        System.out.print("主题："+recordMetadata.topic()+"\n 分区："+recordMetadata.partition());
                    }
                }
            });
        }

        //3 关闭资源
        kafkaProducer.close();
    }
}
