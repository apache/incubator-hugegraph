package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSerializer;

public class SerializerFactory {

    public static AbstractSerializer serializer(String name, HugeGraph graph) {
//        if (name.equalsIgnoreCase("binary")) {
//            return new BinarySerializer(graph);
//        }
//        else if (name.equalsIgnoreCase("text")) {
//            return new TextSerializer(graph);
//        }
        // TODO return registered serializer
        return new CassandraSerializer(graph);
    }
}
