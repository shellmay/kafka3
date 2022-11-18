package org.yumeko.kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Optional;

public class MyConsumer {
    @KafkaListener(topics = "topic-spring")
    public void onMessage(ConsumerRecord<Integer, String> record) {
        Optional<ConsumerRecord<Integer, String>> optional =
                Optional.ofNullable(record);
        if (optional.isPresent()) {
            System.out.println(
                    record.topic() + "\t"
                            + record.partition() + "\t"
                            + record.offset() + "\t"
                            + record.key() + "\t"
                            + record.value());
        }
    }
}
