package com.baidu.hugegraph2.backend.serializer;

import com.baidu.hugegraph2.HugeGraph;

public class SerializerFactory {

    public static AbstractSerializer serializer(String name, HugeGraph graph) {
        if (name.equalsIgnoreCase("binary")) {
            return new BinarySerializer(graph);
        }
        else if (name.equalsIgnoreCase("text")) {
            return new TextSerializer(graph);
        }
        return null;
    }
}
