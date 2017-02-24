package com.baidu.hugegraph2.backend.store;

import java.util.List;

/**
 * Created by jishilei on 17/3/19.
 */
public class StoreMutation extends Mutation<DBEntry, Object> {

    public StoreMutation() {
    }

    public StoreMutation(List<DBEntry> additions, List<Object> deletions) {
        super(additions, deletions);
    }

}
