package org.yumeko.kafka.producer.CustomInterceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.yumeko.kafka.producer.CustomSerialization.User;
import org.yumeko.kafka.producer.CustomSerialization.UserSerializer;

import java.util.HashMap;
import java.util.Map;

public class MyProducerThree {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.134:9092");
        // 设置拦截器
        configs.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "org.yumeko.kafka.producer.CustomInterceptor.InterceptorOne," +
                "org.yumeko.kafka.producer.CustomInterceptor.InterceptorTwo," +
                "org.yumeko.kafka.producer.CustomInterceptor.InterceptorThree");
        // 设置⾃定义的序列化类
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);
        // 设置⾃定义分区器
        // configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        configs.put("partitioner.class", "org.yumeko.kafka.producer.CustomPartitioner.CustomPartitioner");
        //该选项控制着已发送消息的持久性。
        //acks=0 ：⽣产者不等待broker的任何消息确认。只要将消息放到了socket的缓冲区，就认为消息已发送。不能保证服务器是否收
        //到该消息， retries 设置也不起作⽤，因为客户端不关⼼消息是否发送失败。客户端收到的消息偏移量永远是-1。
        //acks=1 ：leader将记录写到它本地⽇志，就响应客户端确认消息，⽽不等待follower副本的确认。如果leader确认了消息就宕机，
        //则可能会丢失消息，因为follower副本可能还没来得及同步该消息。
        //acks=all ：leader等待所有同步的副本确认该消息。保证了只要有⼀个同步副本存在，消息就不会丢失。这是最强的可⽤性保
        //证。等价于 acks=-1 。默认值为1，字符串。可选值：[all, -1, 0, 1]
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        //设置该属性为⼀个⼤于1的值，将在消息发送失败的时候重新发送消息。该重试与客户端收到异常重新发送并⽆⼆⾄。允许重试但是
        //不设置 max.in.flight.requests.per.connection 为1，存在消息乱序的可能，因为如果两个批次发送到同⼀个分区，第⼀
        //个失败了重试，第⼆个成功了，则第⼀个消息批在第⼆个消息批后。int类型的值，默认：0，可选值：[0,...,2147483647]
        configs.put("retries", 1);
        //⽣产者⽣成数据的压缩格式。默认是none（没有压缩）。允许的值： none ， gzip ， snappy 和 lz4 。压缩是对整个消息批次来
        //讲的。消息批的效率也影响压缩的⽐例。消息批越⼤，压缩效率越好。字符串类型的值。默认是none。
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        KafkaProducer<String, User> producer = new KafkaProducer<>(configs);
        User user = new User();
        user.setUserId(1001);
        user.setUsername("张三");

        ProducerRecord<String, User> record = new ProducerRecord<>("tp_interceptor", 0, user.getUsername(), user);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("消息发送成功：" + metadata.topic() + "\t" + metadata.partition() + "\t" + metadata.offset());
            } else {
                System.out.println("消息发送异常");
            }
        });
        // 关闭⽣产者
        producer.close();
    }
}
