package org.apache.hugegraph.pd.store;

import org.apache.hugegraph.pd.raft.KVStoreClosure;

import com.baidu.hugegraph.pd.grpc.Pdpb;

public abstract class BaseKVStoreClosure implements KVStoreClosure {
    private Pdpb.Error error;
    private Object data;
    @Override
    public Pdpb.Error getError() {
        return error;
    }

    @Override
    public void setError(Pdpb.Error error) {
        this.error = error;
    }

    @Override
    public Object getData() {
        return data;
    }

    @Override
    public void setData(Object data) {
        this.data = data;
    }


}
