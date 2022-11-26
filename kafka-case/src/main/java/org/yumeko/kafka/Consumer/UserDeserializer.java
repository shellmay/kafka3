package org.yumeko.kafka.Consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.yumeko.kafka.producer.CustomSerialization.User;

import java.nio.ByteBuffer;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        ByteBuffer allocate = ByteBuffer.allocate(data.length);
        allocate.put(data);
        allocate.flip();
        int userId = allocate.getInt();
        int length = allocate.getInt();
        System.out.println(length);
        String username = new String(data, 8, length);
        return new User(userId, username);
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
