package org.apache.hugegraph.pd.client.rpc;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.grpc.PDGrpc;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhangyingjie
 * @date 2024/1/31
 **/
public class InvokeProxy {

    @Getter
    @Setter
    private volatile PDGrpc.PDBlockingStub stub;
    @Getter
    @Setter
    private String leader;
    @Getter
    private List<String> hosts;

    public InvokeProxy(String[] switcher) {
        updateHosts(switcher);
    }

    private void updateHosts(String[] switcher) {
        List l = new ArrayList<>(switcher.length);
        for (String host : switcher) {
            if (!host.isEmpty()) {
                l.add(host);
            }
        }
        this.hosts = l;
    }
}
