package com.baidu.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    protected StandardConsumer(Properties props) {
        super(props);
    }

    @Override
    public void consume() {
        ConsumerRecords<String, ByteBuffer> records = this.consumer.poll(Duration.ofMillis(1000));
        if (records.count() > 0) {
           for(ConsumerRecord<String, ByteBuffer> record : records.records(this.topic)) {
               System.out.println(String.format("Going to consumer [%s] - %s", record.key().toString(), new String(record.value().array()) ));
           }
        }
        consumer.commitAsync();
    }
    
}
