package com.baidu.hugegraph2.backend.store;

import java.util.Collection;

import com.baidu.hugegraph2.backend.id.Id;

public interface BackendEntry {

    public static class BackendColumn {
        public byte[] name;
        public byte[] value;
    }

    public Id id();
    public void id(Id id);

    public Collection<BackendColumn> columns();
    public void columns(Collection<BackendColumn> columns);
}
