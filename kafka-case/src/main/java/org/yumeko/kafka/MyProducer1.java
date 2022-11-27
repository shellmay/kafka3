package org.yumeko.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MyProducer1 {
    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        Map<String, Object> configs = new HashMap();
        // 设置连接Kafka的初始连接⽤到的服务器地址
        // 如果是集群，则可以通过此初始连接发现集群中的其他broker
        configs.put("bootstrap.servers", "192.168.1.102:9092");
        // 设置key的序列化器
        configs.put("key.serializer", IntegerSerializer.class);
        // 设置value的序列化
        configs.put("value.serializer", StringSerializer.class);
        //configs.put("acks", "1");
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        List<Header> header = new ArrayList<>();
        header.add(new RecordHeader("kafka.name", "Producer".getBytes()));

        // ⽤于封装Producer的消息
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                "topic_1", // 主题名称
                0,// 分区编号，现在只有⼀个分区，所以是0
                System.currentTimeMillis(), //
                0, // 数字作为key
                "hell kafka", // 字符串作为value
                header
        );
        // 发送消息，同步等待消息的确认
        final RecordMetadata metadata = producer.send(record).get(3000, TimeUnit.MILLISECONDS);
        System.out.println(metadata.topic());
        // 关闭⽣产者
        producer.close();
    }
}
