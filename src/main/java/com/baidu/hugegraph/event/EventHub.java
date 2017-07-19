package com.baidu.hugegraph.event;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.type.ExtendableIterator;
import com.baidu.hugegraph.util.E;

public class EventHub {

    public static final String ANY_EVENT = "*";

    private static final Logger logger =
            LoggerFactory.getLogger(EventHub.class);

    private String name;
    private Map<String, List<EventListener>> listeners;
    private ExecutorService executor;

    public EventHub() {
        this("hub");
    }

    public EventHub(String name) {
        this.name = name;
        this.listeners = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(1);
    }

    public String name() {
        return this.name;
    }

    public boolean containsListener(String event) {
        return this.listeners.containsKey(event);
    }

    public List<EventListener> listeners(String event) {
        return Collections.unmodifiableList(this.listeners.get(event));
    }

    public void listen(String event, EventListener listener) {
        E.checkNotNull(event, "event");
        E.checkNotNull(listener, "event listener");

        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            ls = new LinkedList<>();
            this.listeners.put(event, ls);
        }
        ls.add(listener);
    }

    public List<EventListener> unlisten(String event) {
        return Collections.unmodifiableList(this.listeners.remove(event));
    }

    public boolean unlisten(String event, EventListener listener) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            return false;
        }
        return ls.remove(listener);
    }

    public void notify(String event, @Nullable Object... args) {
        ExtendableIterator<EventListener> all = new ExtendableIterator<>();

        List<EventListener> ls = this.listeners.get(event);
        if (ls != null && !ls.isEmpty()) {
            all.extend(ls.iterator());
        }
        List<EventListener> lsAny = this.listeners.get(ANY_EVENT);
        if (lsAny != null && !lsAny.isEmpty()) {
            all.extend(lsAny.iterator());
        }

        if (!all.hasNext()) {
            return;
        }

        Event ev = new Event(this, event, args);
        this.executor.submit(() -> {
            // Notify all listeners, and ignore the results
            while (all.hasNext()) {
                try {
                    all.next().event(ev);
                } catch (Throwable e) {
                    logger.warn("Failed to handle event: {}", ev, e);
                }
            }
        });
    }

    public Object call(String event, @Nullable Object... args) {
        List<EventListener> ls = this.listeners.get(event);
        if (ls == null) {
            throw new RuntimeException("Not found listener for: " + event);
        } else if (ls.size() != 1) {
            throw new RuntimeException("Too many listeners for: " + event);
        }
        EventListener listener = ls.get(0);
        return listener.event(new Event(this, event, args));
    }
}
