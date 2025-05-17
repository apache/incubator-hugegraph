package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.Closure;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.Errors;

public interface KVStoreClosure extends Closure {

    Errors getError();

    void setError(final Errors error);

    Object getData();

    void setData(final Object data);
}