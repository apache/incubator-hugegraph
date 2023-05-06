package org.apache.hugegraph.pd.client;

import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;

import java.util.LinkedList;

/**
 * @author zhangyingjie
 * @date 2022/6/20
 **/
public class AbstractClientStubProxy {

    private AbstractBlockingStub blockingStub;
    private AbstractStub stub;

    public LinkedList<String> getHostList() {
        return hostList;
    }

    private LinkedList<String> hostList = new LinkedList<>();

    public AbstractClientStubProxy(String[] hosts) {
        for (String host : hosts) if (!host.isEmpty()) hostList.offer(host);
    }

    public String nextHost() {
        String host = hostList.poll();
        hostList.offer(host);   //移到尾部
        return host;
    }

    public void setBlockingStub(AbstractBlockingStub stub) {
        this.blockingStub = stub;
    }

    public AbstractBlockingStub getBlockingStub() {
        return this.blockingStub;
    }

    public String getHost() {
        return hostList.peek();
    }

    public int getHostCount() {
        return hostList.size();
    }

    public AbstractStub getStub() {
        return stub;
    }

    public void setStub(AbstractStub stub) {
        this.stub = stub;
    }
}
