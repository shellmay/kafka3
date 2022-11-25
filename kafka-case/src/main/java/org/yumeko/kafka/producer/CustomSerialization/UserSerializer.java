package org.yumeko.kafka.producer.CustomSerialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

//由于Kafka中的数据都是字节数组，在将消息发送到Kafka之前需要先将数据序列化为字节数组。
//序列化器的作⽤就是⽤于序列化要发送的消息的。
//Kafka使⽤ org.apache.kafka.common.serialization.Serializer 接⼝⽤于定义序列化器，将泛型指定类型的数据转换为字节数组。
//⼀般⽣产中使⽤avro。
//⾃定义序列化器需要实现org.apache.kafka.common.serialization.Serializer<T>接⼝，
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, User data) {
        try {
            // 如果数据是null，则返回null
            if (data == null) return null;
            Integer userId = data.getUserId();
            String username = data.getUsername();
            int length = 0;
            byte[] bytes = null;
            if (null != username) {
                bytes = username.getBytes("utf-8");
                length = bytes.length;
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + length);
            buffer.putInt(userId);
            buffer.putInt(length);
            buffer.put(bytes);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("序列化数据异常");
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, User data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
