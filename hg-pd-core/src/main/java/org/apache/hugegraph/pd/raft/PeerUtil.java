package org.apache.hugegraph.pd.raft;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.hugegraph.pd.common.KVPair;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class PeerUtil {
    /**
     * 只比较 ip 和 port
     * @param p1
     * @param p2
     * @return
     */
    public static boolean isPeerEquals(PeerId p1, PeerId p2) {
        if (p1 == null && p2 == null) {
            return true;
        }
        if (p1 == null || p2 == null) {
            return false;
        }
        return Objects.equals(p1.getIp(), p2.getIp()) && Objects.equals(p1.getPort(), p2.getPort());
    }

    public static List<KVPair<String, PeerId>> parseConfig(String conf) {
        List<KVPair<String, PeerId>> result = new LinkedList<>();

        if (conf != null && conf.length() > 0) {
            for (var s : conf.split(",")) {
                if (s.endsWith("/leader")) {
                    result.add(new KVPair<>("leader", JRaftUtils.getPeerId(s.substring(0, s.length() - 7))));
                } else if (s.endsWith("/learner")) {
                    result.add(new KVPair<>("learner", JRaftUtils.getPeerId(s.substring(0, s.length() - 8))));
                } else if (s.endsWith("/follower")) {
                    result.add(new KVPair<>("follower", JRaftUtils.getPeerId(s.substring(0, s.length() - 9))));
                } else {
                    result.add(new KVPair<>("follower", JRaftUtils.getPeerId(s)));
                }
            }
        }

        return result;
    }
}
