package com.baidu.hugegraph.kafka.consumer;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class StandardConsumer extends ConsumerClient<String, byte[]> {

    protected StandardConsumer(Properties props) {
        super(props);
    }

    @Override
    public void consume() {
        ConsumerRecords<String, byte[]> records = this.consumer.poll(Duration.ofMillis(10000));
        if (records.count() > 0) {
           for(ConsumerRecord<String, byte[]> record : records.records(this.topic)) {
               System.out.println(String.format("Going to consumer [%s] - %s", record.key().toString(), new String(record.value())));
           }
        }
        consumer.commitAsync();
    }
    
}
