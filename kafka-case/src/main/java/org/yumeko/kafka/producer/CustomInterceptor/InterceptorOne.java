package org.yumeko.kafka.producer.CustomInterceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

//onSend(ProducerRecord)：该⽅法封装进KafkaProducer.send⽅法中，即运⾏在⽤户主线程中。Producer 确保在消息被序列化以计算分区前调⽤该⽅法。
//⽤户可以在该⽅法中对消息做任何操作，但最好保证不要修 改消息所属的topic和分区，否则会影响⽬标分区的计算。
//onAcknowledgement(RecordMetadata, Exception)：该⽅法会在消息被应答之前或消息发送失败时调⽤，并且通常都是在Producer回调逻辑触发之前。
//onAcknowledgement运⾏在Producer的IO线程中，因此不 要在该⽅法中放⼊很重的逻辑，否则会拖慢Producer的消息发送
//Interceptor可能被运⾏在多个线程中，因此在具体实现时⽤户需要⾃⾏确保线程安全。
//另外倘若指定了多个Interceptor，则Producer将按照指定顺序调⽤它们，并仅仅是捕获每个Interceptor可能抛出的异常记录到错误⽇志中⽽⾮在向上传递。
//这在使⽤过程中要特别留意。
public class InterceptorOne<KEY, VALUE> implements ProducerInterceptor<KEY, VALUE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterceptorOne.class);

    @Override
    public ProducerRecord<KEY, VALUE> onSend(ProducerRecord<KEY, VALUE> record) {
        System.out.println("拦截器1---go");
        // 此处根据业务需要对相关的数据作修改
        String topic = record.topic();
        Integer partition = record.partition();
        Long timestamp = record.timestamp();
        KEY key = record.key();
        VALUE value = record.value();
        Headers headers = record.headers();
        // 添加消息头
        headers.add("interceptor", "interceptorOne".getBytes());
        ProducerRecord<KEY, VALUE> newRecord = new ProducerRecord<KEY, VALUE>(
                topic,
                partition,
                timestamp,
                key,
                value,
                headers
        );
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("拦截器1---back");
        if (exception != null) {
            // 如果发⽣异常，记录⽇志中
            LOGGER.error(exception.getMessage());
        }
    }


    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
