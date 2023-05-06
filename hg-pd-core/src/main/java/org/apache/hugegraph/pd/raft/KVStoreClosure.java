package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.Closure;
import com.baidu.hugegraph.pd.grpc.Pdpb;

public interface KVStoreClosure extends Closure {

    Pdpb.Error getError();

    void setError(final Pdpb.Error error);

    Object getData();

    void setData(final Object data);
}