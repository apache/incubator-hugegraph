package com.baidu.hugegraph.backend.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;

public abstract class AbstractBackendStoreProvider
                implements BackendStoreProvider {

    protected String name;
    protected Map<String, BackendStore> stores;

    private EventHub storeEventHub = new EventHub("store");

    @Override
    public void listen(EventListener listener) {
        this.storeEventHub.listen(EventHub.ANY_EVENT, listener);
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void open(String name) {
        E.checkNotNull(name, "store name");

        this.name = name;
        this.stores = new ConcurrentHashMap<>();

        this.storeEventHub.notify(Events.STORE_OPEN, this);
    }

    @Override
    public void close() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            // TODO: catch exceptions here
            store.close();
        }
        this.storeEventHub.notify(Events.STORE_CLOSE, this);
    }

    @Override
    public void init() {
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
        this.storeEventHub.notify(Events.STORE_INIT, this);
    }

    @Override
    public void clear() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.clear();
        }
        this.storeEventHub.notify(Events.STORE_CLEAR, this);
    }
}
