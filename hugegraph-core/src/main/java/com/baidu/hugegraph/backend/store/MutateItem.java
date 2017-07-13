package com.baidu.hugegraph.backend.store;

/**
 * Created by liningrui on 2017/7/17.
 */
public class MutateItem {

    private BackendEntry entry;

    private MutateAction type;

    public static MutateItem of(BackendEntry entry, MutateAction type) {
        return new MutateItem(entry, type);
    }

    public MutateItem(BackendEntry entry, MutateAction type) {
        this.entry = entry;
        this.type = type;
    }

    public BackendEntry entry() {
        return entry;
    }

    public void entry(BackendEntry entry) {
        this.entry = entry;
    }

    public MutateAction type() {
        return type;
    }

    public void type(MutateAction type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return String.format("entry: %s, type: %s", entry, type);
    }

}
