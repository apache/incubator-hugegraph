package com.baidu.hugegraph2.backend.store;

import java.util.List;

/**
 * Created by jishilei on 17/3/19.
 */
public class BackendStoreMutation extends Mutation<BackendEntry, Object> {

    public BackendStoreMutation() {
    }

    public BackendStoreMutation(List<BackendEntry> additions, List<Object> deletions) {
        super(additions, deletions);
    }

}
