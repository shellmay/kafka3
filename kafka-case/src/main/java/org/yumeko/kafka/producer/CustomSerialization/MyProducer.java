package org.yumeko.kafka.producer.CustomSerialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class MyProducer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.134:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 设置⾃定义的序列化类
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(configs);
        User user = new User();
        user.setUserId(1001);
        user.setUsername("张三");
        ProducerRecord<String, User> record = new ProducerRecord<>(
                "tp_serializer",
                0,
                user.getUsername(),
                user
        );
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("消息发送成功："
                        + metadata.topic() + "\t"
                        + metadata.partition() + "\t"
                        + metadata.offset());
            } else {
                System.out.println("消息发送异常");
            }
        });
        // 关闭⽣产者
        producer.close();
    }
}
