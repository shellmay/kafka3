package org.yumeko.kafka.producer.CustomPartitioner;

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
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "tp_partitioner",
                0,
                "1",
                "自定义分区"
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
