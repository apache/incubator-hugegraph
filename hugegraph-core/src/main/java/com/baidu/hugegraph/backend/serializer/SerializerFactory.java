package com.baidu.hugegraph.backend.serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;

public class SerializerFactory {

    private static Map<String, Class<? extends AbstractSerializer>> serializers;

    static {
        serializers = new ConcurrentHashMap<>();
    }

    public static AbstractSerializer serializer(String name, HugeGraph graph) {
        if (name.equalsIgnoreCase("binary")) {
            return new BinarySerializer(graph);
        }
        else if (name.equalsIgnoreCase("text")) {
            return new TextSerializer(graph);
        }

        Class<? extends AbstractSerializer> clazz = serializers.get(name);
        if (clazz == null) {
            throw new BackendException(String.format(
                    "Not exists serializer: %s", name));
        }

        assert AbstractSerializer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor(HugeGraph.class).newInstance(graph);
        } catch (Exception e) {
            throw new BackendException(e);
        }
    }

    public static void register(String name, String classPath) {
        ClassLoader classLoader = SerializerFactory.class.getClassLoader();
        Class<?> clazz = null;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (ClassNotFoundException e) {
            throw new BackendException(e);
        }

        // check subclass
        if (!AbstractSerializer.class.isAssignableFrom(clazz)) {
            throw new BackendException(String.format(
                    "Class '%s' is not a subclass of class AbstractSerializer",
                    classPath));
        }

        // check exists
        if (serializers.containsKey(name)) {
            throw new BackendException(String.format(
                    "Exists serializer: %s(Class '%s')",
                    name, serializers.get(name).getName()));
        }

        // register class
        serializers.put(name, (Class) clazz);
    }
}
