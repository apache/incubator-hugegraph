package com.baidu.hugegraph.backend.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.memory.InMemoryDBStoreProvider;

public class BackendProviderFactory {

    private static Map<String, Class<? extends BackendStoreProvider>> storeProviders;

    static {
        storeProviders = new ConcurrentHashMap<>();
    }

    public static BackendStoreProvider open(String backend, String name) {
        if (backend.equalsIgnoreCase("memory")) {
            return new InMemoryDBStoreProvider(name);
        }

        Class<? extends BackendStoreProvider> clazz = storeProviders.get(backend);
        if (clazz == null) {
            throw new BackendException(
                      "Not exists BackendStoreProvider: %s", backend);
        }

        assert BackendStoreProvider.class.isAssignableFrom(clazz);
        BackendStoreProvider instance = null;
        try {
            instance = clazz.newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }
        instance.open(name);
        return instance;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = BackendProviderFactory.class.getClassLoader();
        Class<?> clazz = null;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException(e);
        }

        // Check subclass
        if (!BackendStoreProvider.class.isAssignableFrom(clazz)) {
            throw new BackendException("Class '%s' is not a subclass of " +
                                       "class BackendStoreProvider", classPath);
        }

        // Check exists
        if (storeProviders.containsKey(name)) {
            throw new BackendException(
                      "Exists BackendStoreProvider: %s(Class '%s')",
                      name, storeProviders.get(name).getName());
        }

        // Register class
        storeProviders.put(name, (Class) clazz);
    }
}
