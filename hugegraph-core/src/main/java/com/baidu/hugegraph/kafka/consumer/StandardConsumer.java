package com.baidu.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Properties;

import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopic;
import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopicBuilder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    protected StandardConsumer(Properties props) {
        super(props);

    }
    
}
