package com.baidu.hugegraph.event;

public interface EventListener extends java.util.EventListener {
    /**
     * The event callback
     * @param event object
     * @return event result
     */
    public Object event(Event event);
}
