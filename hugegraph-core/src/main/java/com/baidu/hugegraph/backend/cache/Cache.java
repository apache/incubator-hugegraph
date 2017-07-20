package com.baidu.hugegraph.backend.cache;

import java.util.function.Function;

import com.baidu.hugegraph.backend.id.Id;

public interface Cache {

    public Object get(Id id);

    public Object getOrFetch(Id id, Function<Id, Object> fetcher);

    public void update(Id id, Object value);

    public void updateIfAbsent(Id id, Object value) ;

    public void invalidate(Id id);

    public void clear();

    public void expire(long seconds);

    public void tick();

    public long capacity();

    public long size();
}
