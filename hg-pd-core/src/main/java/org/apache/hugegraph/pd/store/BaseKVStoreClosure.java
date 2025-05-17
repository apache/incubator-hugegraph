package org.apache.hugegraph.pd.store;

import com.alipay.sofa.jraft.Status;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.raft.KVStoreClosure;

public abstract class BaseKVStoreClosure implements KVStoreClosure {
    private Errors error;
    private Object data;
    @Override
    public Errors getError() {
        return error;
    }

    @Override
    public void setError(Errors error) {
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
