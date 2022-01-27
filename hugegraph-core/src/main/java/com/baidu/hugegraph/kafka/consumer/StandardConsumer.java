package com.baidu.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.Properties;


public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    protected StandardConsumer(Properties props) {
        super(props);

    }
    
}
